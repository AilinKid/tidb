package main

import (
	"fmt"
	"sync"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	DB0, _ := sql.Open("mysql", "root:@tcp(127.0.0.1:4329)/test")
	DB1, _ := sql.Open("mysql", "root:@tcp(127.0.0.1:4329)/test")
	DB2, _ := sql.Open("mysql", "root:@tcp(127.0.0.1:4329)/test")
	DB3, _ := sql.Open("mysql", "root:@tcp(127.0.0.1:4329)/test")
	//设置数据库最大连接数
	DB0.SetConnMaxLifetime(1000)
	DB1.SetConnMaxLifetime(1000)
	DB2.SetConnMaxLifetime(1000)
	DB3.SetConnMaxLifetime(1000)
	//设置上数据库最大闲置连接数
	DB0.SetMaxIdleConns(10)
	DB1.SetMaxIdleConns(10)
	DB2.SetMaxIdleConns(10)
	DB3.SetMaxIdleConns(10)
	//验证连接
	fmt.Println("connnect success")
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		ok := true
		// sequence_id_1_range [0,196608) 1/64 of total
		// sequence_id_2_range [196608, 196608) 1/64 of total
		sequence_id_1_base := 0
		sequence_id_2_base := 196608
		sql := fmt.Sprintf("insert into log_link_visit_action3 values")
		for i := 0; i < 4194304; i++ {
			if i%131072 == 0 {
				fmt.Printf("insert DB0 progress %d/32!", i/131072)
			}
			if i%1000 == 0 || i == 4194303 {
				sql += fmt.Sprintf("(%d, %d, 1,1)", i%196608+sequence_id_1_base, i%196608+sequence_id_2_base)
				_, err := DB0.Exec(sql)
				if err != nil {
					fmt.Println("insert DB0 Fail!", err.Error())
					ok = false
					break
				}
				sql = fmt.Sprintf("insert into log_link_visit_action3 values")
			} else {
				sql += fmt.Sprintf("(%d, %d, 1,1),", i%196608+sequence_id_1_base, i%196608+sequence_id_2_base)
			}
		}
		wg.Done()
		if ok {
			fmt.Println("insert DB0 !")
		}
	}()
	go func() {
		ok := true
		// sequence_id_1_range [0,196608) 1/64 of total
		// sequence_id_2_range [196608, 196608) 1/64 of total
		sequence_id_1_base := 0
		sequence_id_2_base := 196608
		sql := fmt.Sprintf("insert into log_link_visit_action3 values")
		for i := 0; i < 4194304; i++ {
			if i%131072 == 0 {
				fmt.Printf("insert DB1 progress %d/32!", i/131072)
			}
			if i%1000 == 0 || i == 4194303 {
				sql += fmt.Sprintf("(%d, %d, 1,1)", i%196608+sequence_id_1_base, i%196608+sequence_id_2_base)
				_, err := DB0.Exec(sql)
				if err != nil {
					fmt.Println("insert DB1 Fail!", err.Error())
					ok = false
					break
				}
				sql = fmt.Sprintf("insert into log_link_visit_action3 values")
			} else {
				sql += fmt.Sprintf("(%d, %d, 1,1),", i%196608+sequence_id_1_base, i%196608+sequence_id_2_base)
			}
		}
		wg.Done()
		if ok {
			fmt.Println("insert DB1 success!")
		}
	}()
	go func() {
		ok := true
		// sequence_id_1_range [0,196608) 1/64 of total
		// sequence_id_2_range [196608, 196608) 1/64 of total
		sequence_id_1_base := 0
		sequence_id_2_base := 196608
		sql := fmt.Sprintf("insert into log_link_visit_action3 values")
		for i := 0; i < 4194304; i++ {
			if i%131072 == 0 {
				fmt.Printf("insert DB2 progress %d/32!", i/131072)
			}
			if i%1000 == 0 || i == 4194303 {
				sql += fmt.Sprintf("(%d, %d, 1,1)", i%196608+sequence_id_1_base, i%196608+sequence_id_2_base)
				_, err := DB0.Exec(sql)
				if err != nil {
					fmt.Println("insert DB2 Fail!", err.Error())
					ok = false
					break
				}
				sql = fmt.Sprintf("insert into log_link_visit_action3 values")
			} else {
				sql += fmt.Sprintf("(%d, %d, 1,1),", i%196608+sequence_id_1_base, i%196608+sequence_id_2_base)
			}
		}
		wg.Done()
		if ok {
			fmt.Println("insert DB2 success!")
		}
	}()
	wg.Wait()
	fmt.Println("insert over!")
}
