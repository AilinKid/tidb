package rule

// dev guide for session variable detecting.
// since session variable is quite independent of subtree bound from pattern, we could do this check when picking
//
//
// dev guide for PreCheck
//
// like what we did in `xf_decorrelate_apply_base`from abstract base rule, since currently the no_decorrelate
// variable is recorded in very apply operator itself. When we want to check some attribute in the corresponding
// operator matched from your pattern, you can only do it after binding via PreCheck or XForm. In the case above
// we just check apply.NoDecorrelate side, and shut down for all apply related rule when this variable is switched
// on.
// Like you may ask why we couldn't move this check forward to the getValidRules because there can also see the
// OptGroupExpressionTask.groupExpression is an apply operator as well. Two reasons.
// 1: task pkg only care about the task push and pop and simple rule pick, the rule check logic should not be
//    injected into it.
// 2: rules rooted from an apply operator not only include de-correlated ones, some others may also work.
//
//
// dev guide for XForm:
//
// 1: do edit any element in the holder, which is bound from memo, one it changed, the op and the wrapper GE
// couldn't align with its hash64 which is already registered in group and memo map. Use the cloned one, care
// with the shallow copy.
//
// 2: since logical plan pointer can refer to a logical operator and group expression inside memo, the prev one
// indicates this node is barely a newly created one, which haven't been added to a group. the later one means
// the node didn't change in this xForm, we can safely use the group info it attached.
//
// 3: Distinguishing the var inside xForm with GE suffix or bare OP Type by naming, which will be clear line when
// encapsulating the transform tree bottom up, and we can easily tell which part is unchanged, and which part is
// newly created.
