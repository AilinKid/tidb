package rule

import (
	"container/list"
	"github.com/pingcap/tidb/pkg/planner/memo"
	"github.com/pingcap/tidb/pkg/planner/pattern"
)

// Document
//
// Binder is a structure used to bind the sub logical plan with the special pattern. Since current
// logical plans are a tree structure, while it's input are child groups, so it's actually been a tree
// enumeration work from a forest represented by a memo Group.
//
// Why not choose recursive way to do the binding work? Because the recursive way is hard to control
// the iteration order, and it's hard to do the backtracking work when all current group expressions
// couldn't match the pattern (we call them an exhaustion).
//
// Like:
//         G1[a1,a2,a3]                           P1{ANY1}
//        /      ▴    \             <==>         /        \
// G2[b1,b2,b3]    G3[c1,c2,c3]              P2{ANY1}, P3{SPECIFIC3}
//        ▴                  ▴
// when G1 is pinned at a2, G2 is pinned at b2, G3 is pinned at c3 and c3 is just not matched with the
// child pattern SPECIFIC3, then we need to left-track to G2, next to next valid b3 if any, and try to
// re-enter G3 to do the matching flow starting from 0. Another case, we say G2 is pinned at b3 and
// there is no next element in G2, then we need to backtrack to G1, next to a3 if any, and re-enter G2
// and G3 to do the enumeration clearly.
//
// As we can see, the recursive calling need to return to status/signal/control to left-brother, or upper
// caller to tell them to iterate to next element (if no more, then backtracking again) to do the matching
// work, status saving and restored work will become complex.
//
// So instead, the binder is a stack-based structure, the calling routine is still the recursive way, but
// status info is managed in the stackInfo array in the toppest caller --- Binder itself. So when an exhaustion
// happened in no matter where in the tree, the binder can easily pop out the exhausted group, and restart
// the process from the next element of the closest group.
//
// Like:
//         G1[a1,a2,a3]                           P1{ANY1}
//        /      ▴    \             <==>         /        \
// G2[b1,b2,b3]    G3[c1,c2,c3]              P2{ANY1}, P3{SPECIFIC3}
//        ▴                  ▴
// For the same case above, the Binder's simplest stackInfo array will be like:
// +-------+-------+---------+
// | means | index | offset  |
// +-------+-------+---------+
// |   G1  |   0   |   1     |
// |   G2  |   1   |   1     |
// |   G3  |   2   |   2     |
// +-------+-------+---------+
// which means the G1 is pinned at a2(offset-1), G2 is pinned at b2(offset-1), G3 is pinned at c3(offset-2), and
// when the c3 is not matched and exhausted, we just pop the G3 out of the stack and return back the toppest loop.
// ref Next() for more detail, in toppest loop, we just get the top element of current stack which G2, and iterate
// its element to the next one which is b3 with offset 2.
// +-------+-------+---------+
// | means | index | offset  |
// +-------+-------+---------+
// |   G1  |   0   |   1     |
// |   G2  |   1   |   2     |
// +-------+-------+---------+
// |   G3  |   2   |   0     |
// +-------------------------+
// Then we can re-enter the G1,G2,G3 again to do the matching, since G1 state info is pinned at a2(offset-1), G2 is
// iterated to next b3 (offset-2), these two groups will output the guided group expression from state info. while
// for G3, when re-enter it again, since there is no stack info for it, we will reset the offset to 0, and start
// iterating first element c1 from G3.
//
// this is the first version we think about, since we consider iterating among all group expressions in the group
// is a waste O(n), because many equivalent logical plan are not matched with the pattern, taking their offset into account
// is not necessary.
//
// So we changed the group expressions field as linked list, each element can quickly find their next element, and we
// made some maintain work to make sure that same operand element will be stored continuously in the list, so that we
// only need iterate part of the list to find the matched group expression with O(k) <= O(n). So the version 2 is like:
//
// For the same case above, the Binder's simplest stackInfo array will be like:
// +-------+-------+---------+
// | means | index | *elem   |
// +-------+-------+---------+
// |   G1  |   0   | *elem1  |
// |   G2  |   1   | *elem2  |
// |   G3  |   2   | *elem3  |
// +-------+-------+---------+
// now the stackInfo array is described as []*list.Element, and the element is the first matched group expression element
// inside group. so when we need to iterate the next element, we just call elem.Next() to get the next element, and when
// elem.Next() is not within the same operand or nil, we thought it is beyond continuous part, exhaustion happened. And
// we just pop toppest stack info and return false back to the toppest loop.
// +-------+-------+----------------+
// | means | index | *elem          |
// +-------+-------+----------------+
// |   G1  |   0   | *elem1         |
// |   G2  |   1   | *elem2.Next()  |
// +-------+-------+----------------+
// |   G3  |   2   | *elem3 --> c1  |
// +--------------------------------+
// After we iterate the top element of the stack info to the next element, the stack info should be like the above, and
// G1 is pinned at *elem1 which is pointed to a2, G2 is pinned at *elem2.Next() which is pointed to b3, these two groups
// will output the guided group expression from state info. For G3, when re-enter it again, since there is no stack info
// for it, we will push a new stack info into it, start from first element c1 from G3 provided we say all element in G3
// with the same operand then.
//
// And another problem is about how to generate the matched group expression (part of tree) from the binder:
// 1: assemble them out like a logical plan tree, but this is not necessary, don't waste memory to construct them.
// 2: use the placeholder to hold the matched gE, and it's reused and linked to a new one when last iteration is failed or done.
//
// so we choose the second way, and the placeholder is a dynamic structure, it's a tree structure, cur is value field holding
// match group expression, and subs is the children field holding the matched children group expression, and it's a recursive
// definition. While binder itself is just like a caller, a status saving and driving procedure.

// Sub is dynamic placeholder for *list.element subtree holding which comes from binder process.
type Sub struct {
	cur  *memo.GroupExpression
	subs []*Sub
}

// Binder is leveled status structure used to bind the logical subtree with special given pattern.
type Binder struct {
	// p is the pattern specified by the rule.
	p *pattern.Pattern

	// g is the group to be matched, iterate the group/children's gExpr to find the matched proportion.
	// it can be changed during the binding process, since different gE may have different input groups.
	g *memo.Group

	// gE is the groupExpression to be matched, iterate the group/children's gExpr to find the matched proportion.
	gE *list.Element

	// traceID is the unique id mark of stepping into a group, traced from the root group as stack calling.
	traceID int

	// stackInfo is used to store the current binder's status, it's a map from binderKey(regrading to group) to index
	// value, which is used to tell iterator where to start the next iteration.
	stackInfo []*list.Element

	// expr is the current matched expression dynamically decided during the binder process.
	sub *Sub
}

// NewBinder creates a new Binder.
func NewBinder(p *pattern.Pattern, g *memo.Group) *Binder {
	// if the root group is not matched, then binder is not necessary.
	elem := g.Operand2FirstExpr[p.Operand]
	if elem == nil {
		return nil
	}
	// util now, all the children group and child pattern has been matched, then we can yield a valid top binder.
	return &Binder{
		p:         p,
		g:         g,
		gE:        elem,
		traceID:   0,
		stackInfo: []*list.Element{elem},
		sub:       &Sub{cur: elem.Value.(*memo.GroupExpression)},
	}

}

// 这种 post 机制最丑的地方在于，如果 subs[i+1] next 好好的，subs[i] exhausted, subs[i+1] 的东西也不能用了。
// 最好还是顺序遍历，这样就不会有这种问题。

func Match(p *pattern.Pattern, gE *memo.GroupExpression) bool {
	return false
}

// Next tries to find the next matched group expression from the Binder structure.
// Binder core logic is trying to iterate a matched **Next** concrete logical plan from group tree.
// It will try to match the child pattern with the child group if any, get the matched child group
// expression and return it back to upper caller to form a valid logical plan（across pattern up and down）.
//
// Like：
//
//	     Join
//	    /    \
//		 G1(e1)  G2(e1,e2,e3)
//
// Pattern z: Join{ANY1, ANY2}
//
// When matching a Join pattern z above, current groupExpression's children is Group structure, when we
// want to apply join commutative rule, actually we don't care about what the concrete expression inside
// the group, so for this rule, we don't need to iterate concrete expression inside the G1, G2 group. just
// adding a new join expression with G2 and G1 as children is enough.
//
// Like：
//
//	     Join
//	    /    \
//		 G1(e1)  G2(e1,e2,e3)
//	                / \ (check e?: should be a join operator)
//	               /   \
//			       G3(e5) G4(e6)
//
// Pattern z: Join{ANY1, Join{ANY2, ANY3}}
//
// But for some other rules, like join associativity, we need to iterate the concrete expression inside the
// G2 group to make sure e? should be a Join operator to match the rule requirement, that's means to need to
// pinned G1, then iterate G2's equivalent expression to find the matched Join(e2) like we say, next we got
// a concrete expression: Join(G1, Join(G3, G4)), then we can apply the join associativity rule to transform
// the expression to Join(Join(G1, G3), G4) or other forms.
func (b *Binder) Next() bool {
	var (
		init bool
		ok   bool
	)
	for {
		continueGroup := len(b.stackInfo) - 1
		continueGroupElement := b.stackInfo[continueGroup]
		if !init {
			// first round, we had set the root element to be started, no need to Next again.
			init = true
			// auto inc gE offset inside group to make sure the next iteration will start from the next group expression.
			b.stackInfo[continueGroup] = continueGroupElement.Next()
		}
		ok = b.dfsMatch(b.p, b.sub)
		if ok || len(b.stackInfo) == 1 {
			break
		}
	}
	return ok
}

// dfsMatch tries to match the pattern with the group expression and input groups recursively.
func (b *Binder) dfsMatch(p *pattern.Pattern, parentSub *Sub) bool {
	gE := parentSub.cur
	// quick return for nil group expression, which may come from the upper pickGroupExpression exhaustion.
	if gE == nil {
		return false
	}
	// check if the current group expression is matched.
	if len(p.Children) != len(gE.Children) {
		return false
	}
	// since different group expression may have different children len, we need to make sure the subs
	// is long enough to hold all the children group expression.
	if len(parentSub.subs) < len(p.Children) {
		parentSub.subs = append(parentSub.subs, make([]*Sub, len(p.Children)-len(parentSub.subs))...)
	}
	for i, childPattern := range p.Children {
		// we ensure that pattern len is equal to input child groups len.
		childGroup := gE.Children[i]
		b.traceIn(childPattern, childGroup)
		// rebound the dynamic placeholder no matter whether it is CHANGED or NOT or NIL.
		parentSub.subs[i].cur = b.pickGroupExpression(childPattern)
		// we can sure that childPattern and element in subs[i] is match when arrive here, recursive for child.
		if !b.dfsMatch(childPattern, parentSub.subs[i]) {
			return false
		}
	}
	return true
}

// pickGroupExpression tries to find the next matched group expression from the current group.
func (b *Binder) pickGroupExpression(p *pattern.Pattern) *memo.GroupExpression {
	currentGroup := b.traceID
	currentGroupElement := b.stackInfo[currentGroup]
	if currentGroupElement == nil || !Match(p, currentGroupElement.Value.(*memo.GroupExpression)) {
		// current group has been exhausted, pop out the current group trace info(*element thing) from stackInfo.
		b.stackInfo = b.stackInfo[:currentGroup]
		return nil
	}
	// get the current group expression.
	return currentGroupElement.Value.(*memo.GroupExpression)
}

func (b *Binder) traceIn(p *pattern.Pattern, g *memo.Group) {
	b.traceID++
	for i := len(b.stackInfo); i < b.traceID+1; i++ {
		// for a new stepped-in group, the start iterating index set the first operand element.
		b.stackInfo = append(b.stackInfo, g.Operand2FirstExpr[p.Operand])
	}
}

// Match checks if current pattern matches specified group expression.
func (b *Binder) Match(p *pattern.Pattern, g *memo.GroupExpression) bool {
	// Check if the pattern's Operand is OperandAny and the EngineTypeSet contains the given EngineType.
	if pattern.OperandAny.Match(p.Operand) {
		return true
	}

}

// GetSub returns the current group expression stored in dynamic placeholder element field.
func (b *Binder) GetSub() *Sub {
	return b.sub
