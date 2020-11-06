package main

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

const (
	db_filepath = "./test.db"
)

func TestTransactionTypes(t *testing.T) {
	os.Remove(db_filepath)

	db, err := sql.Open("sqlite3", db_filepath)
	if err != nil {
		t.Fatalf("sql.Open error:%v", err)
		return
	}
	defer db.Close()

	sqlStmt := `
	create table account (id integer not null primary key, name text, balance integer);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		t.Fatalf("%q: %s\n", err, sqlStmt)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("db.Begin error: %v\n", err)
		return
	}
	ins_stmt, err := tx.Prepare("insert into account (id, name, balance) values(?, ?, ?)")
	if err != nil {
		t.Fatalf("tx.Prepare error: %v\n", err)
		return
	}
	defer ins_stmt.Close()

	for i := 0; i < 10; i++ {
		_, err = ins_stmt.Exec(i, fmt.Sprintf("User%03d", i), i*100)
		if err != nil {
			t.Fatalf("insert error: %v\n", err)
			return
		}
	}
	tx.Commit()

	/*
		rows, err := db.Query("select id, name, balance from account")
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id int
			var name string
			var balance int
			err = rows.Scan(&id, &name, &balance)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(id, name, balance)
		}
	*/

	//db.Close()

	test_id := 1
	tests := []struct {
		scene  int
		sql    string
		commit bool
		want   int
	}{
		{scene: 1, sql: "", commit: false, want: test_id * 100},
		{scene: 2, sql: "", commit: false, want: test_id * 100},
		{scene: 1, sql: fmt.Sprintf("update account set balance = balance+1 where id = %v;", test_id), commit: false, want: test_id*100 + 1},
		{scene: 2, sql: "", commit: false, want: test_id * 100}, // Repeatable Read
		{scene: 1, sql: fmt.Sprintf("update account set balance = balance+1 where id = %v;", test_id), commit: true, want: test_id*100 + 2},
		//{scene: 2, sql: "", commit: false, want: test_id*100 + 2},
		//{scene: 2, sql: fmt.Sprintf("update account set balance = balance+10 where id = %v;", test_id), commit: false, want: test_id*100 + 10},
	}

	//conns := make(map[int]*sql.DB)
	txs := make(map[int]*sql.Tx)
	conn := db

	for test_no, test := range tests {
		fmt.Println(test_no)
		//conn, _ := conns[test.scene]
		tx, ok := txs[test.scene]
		if !ok {
			/*
				db, err := sql.Open("sqlite3", db_filepath)
				if err != nil {
					t.Fatalf("sql.Open error:%v", err)
					return
				}
				defer db.Close()
				conns[test.scene] = conn
				conn = db
				_ = conn
			*/

			tx_tmp, err := conn.Begin()
			if err != nil {
				t.Fatalf("conn.Begin error: %v\n", err)
				return
			}
			txs[test.scene] = tx_tmp
			tx = tx_tmp

		}

		if test.sql != "" {
			stmt, err := tx.Prepare(test.sql)
			if err != nil {
				t.Fatalf("%q: %s\n", err, test.sql)
			}
			_, err = stmt.Exec()
			if err != nil {
				t.Fatalf("%q: %s\n", err, test.sql)
			}
		}

		sel_stmt, err := tx.Prepare("select balance from account where id = ?")
		if err != nil {
			t.Fatalf("tx.Prepare error: %v\n", err)
			return
		}
		defer sel_stmt.Close()

		var balance int
		err = sel_stmt.QueryRow(fmt.Sprintf("%d", test_id)).Scan(&balance)
		if err != nil {
			t.Fatalf("stmt.QueryRow().Scan() error: %v\n", err)
			return
		}
		//sel_stmt.Close()

		if balance != test.want {
			t.Fatalf("got:%v want %v\n", balance, test.want)
		}

		if test.commit {
			err = tx.Commit()
			if err != nil {
				t.Fatalf("tx.Commit error: %v\n", err)
				return
			}
		}

	}

}
