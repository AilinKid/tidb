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
	wg.Add(4)
	go func() {
		ok := true
		sequence_id_1_base := 0
		sequence_id_2_base := 12582912
		sql := fmt.Sprintf("insert into log_link_visit_action values")
		for i := 0; i < 3145728; i++ {
			if i%98304 == 0 {
				fmt.Printf("insert DB0 progress %d/32!", i/98304)
			}
			if i%1000 == 0 || i == 3145727 {
				sql += fmt.Sprintf("(%d, %d, 1,1)", i+sequence_id_1_base, i+sequence_id_2_base)
				_, err := DB0.Exec(sql)
				if err != nil {
					fmt.Println("insert DB0 Fail!", err.Error())
					ok = false
					break
				}
				sql = fmt.Sprintf("insert into log_link_visit_action values")
			} else {
				sql += fmt.Sprintf("(%d, %d, 1,1),", i+sequence_id_1_base, i+sequence_id_2_base)
			}
		}
		wg.Done()
		if ok {
			fmt.Println("insert DB0 !")
		}
	}()
	go func() {
		ok := true
		sequence_id_1_base := 3145728
		sequence_id_2_base := 15728640 // 12582912 + 3145728
		sql := fmt.Sprintf("insert into log_link_visit_action values")
		for i := 0; i < 3145728; i++ {
			if i%98304 == 0 {
				fmt.Printf("insert DB1 progress %d/32!", i/98304)
			}
			if i%1000 == 0 || i == 3145727 {
				sql += fmt.Sprintf("(%d, %d, 1,1)", i+sequence_id_1_base, i+sequence_id_2_base)
				_, err := DB0.Exec(sql)
				if err != nil {
					fmt.Println("insert DB1 Fail!", err.Error())
					ok = false
					break
				}
				sql = fmt.Sprintf("insert into log_link_visit_action values")
			} else {
				sql += fmt.Sprintf("(%d, %d, 1,1),", i+sequence_id_1_base, i+sequence_id_2_base)
			}
		}
		wg.Done()
		if ok {
			fmt.Println("insert DB1 success!")
		}
	}()
	go func() {
		ok := true
		sequence_id_1_base := 6291456
		sequence_id_2_base := 18874368 // 12582912 + 6291456
		sql := fmt.Sprintf("insert into log_link_visit_action values")
		for i := 0; i < 3145728; i++ {
			if i%98304 == 0 {
				fmt.Printf("insert DB2 progress %d/32!", i/98304)
			}
			if i%1000 == 0 || i == 3145727 {
				sql += fmt.Sprintf("(%d, %d, 1,1)", i+sequence_id_1_base, i+sequence_id_2_base)
				_, err := DB0.Exec(sql)
				if err != nil {
					fmt.Println("insert DB2 Fail!", err.Error())
					ok = false
					break
				}
				sql = fmt.Sprintf("insert into log_link_visit_action values")
			} else {
				sql += fmt.Sprintf("(%d, %d, 1,1),", i+sequence_id_1_base, i+sequence_id_2_base)
			}
		}
		wg.Done()
		if ok {
			fmt.Println("insert DB2 success!")
		}
	}()
	go func() {
		ok := true
		sequence_id_1_base := 9437184
		sequence_id_2_base := 22020096 // 12582912 + 9437184
		sql := fmt.Sprintf("insert into log_link_visit_action values")
		for i := 0; i < 3145728; i++ {
			if i%98304 == 0 {
				fmt.Printf("insert DB3 progress %d/32!", i/98304)
			}
			if i%1000 == 0 || i == 3145727 {
				sql += fmt.Sprintf("(%d, %d, 1,1)", i+sequence_id_1_base, i+sequence_id_2_base)
				_, err := DB0.Exec(sql)
				if err != nil {
					fmt.Println("insert DB3 Fail!", err.Error())
					ok = false
					break
				}
				sql = fmt.Sprintf("insert into log_link_visit_action values")
			} else {
				sql += fmt.Sprintf("(%d, %d, 1,1),", i+sequence_id_1_base, i+sequence_id_2_base)
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
