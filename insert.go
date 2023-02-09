package main

import (
	"fmt"
	"sync"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	DB0, _ := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/test")
	DB1, _ := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/test")
	DB2, _ := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/test")
	DB3, _ := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/test")
	//设置数据库最大连接数
	DB0.SetConnMaxLifetime(100)
	DB1.SetConnMaxLifetime(100)
	DB2.SetConnMaxLifetime(100)
	DB3.SetConnMaxLifetime(100)
	//设置上数据库最大闲置连接数
	DB0.SetMaxIdleConns(10)
	DB1.SetMaxIdleConns(10)
	DB2.SetMaxIdleConns(10)
	DB3.SetMaxIdleConns(10)
	//验证连接
	fmt.Println("connnect success")
	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		ok := true
		for i := 0; i < 3145728; i++ {
			_, err := DB0.Exec("insert into log_link_visit_action values(nextval(seq), nextval(seq), 1,1);")
			if err != nil {
				fmt.Println("insert DB0 Fail!")
				ok = false
				break
			}
			if i%98304 == 0 {
				fmt.Printf("insert DB0 progress %d/32!", i/98304)
			}
		}
		wg.Done()
		if ok {
			fmt.Println("insert DB0 !")
		}
	}()
	go func() {
		ok := true
		for i := 0; i < 3145728; i++ {
			_, err := DB1.Exec("insert into log_link_visit_action values(nextval(seq), nextval(seq), 1,1);")
			if err != nil {
				fmt.Println("insert DB1 Fail!")
				ok = false
				break
			}
			if i%98304 == 0 {
				fmt.Printf("insert DB1 progress %d/32!", i/98304)
			}
		}
		wg.Done()
		if ok {
			fmt.Println("insert DB1 success!")
		}
	}()
	go func() {
		ok := true
		for i := 0; i < 3145728; i++ {
			_, err := DB2.Exec("insert into log_link_visit_action values(nextval(seq), nextval(seq), 1,1);")
			if err != nil {
				fmt.Println("insert DB2 Fail!")
				ok = false
				break
			}
			if i%98304 == 0 {
				fmt.Printf("insert DB2 progress %d/32!", i/98304)
			}
		}
		wg.Done()
		if ok {
			fmt.Println("insert DB2 success!")
		}
	}()
	go func() {
		ok := true
		for i := 0; i < 3145728; i++ {
			_, err := DB3.Exec("insert into log_link_visit_action values(nextval(seq), nextval(seq), 1,1);")
			if err != nil {
				fmt.Println("insert DB3 Fail!")
				ok = false
				break
			}
			if i%98304 == 0 {
				fmt.Printf("insert DB3 progress %d/32!", i/98304)
			}
		}
		wg.Done()
		if ok {
			fmt.Println("insert DB3 success!")
		}
	}()
	wg.Wait()
	fmt.Println("insert over!")
}
