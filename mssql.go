package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"io/ioutil"
	"log"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

var (
	debug            = flag.Bool("debug", false, "enable debugging")
	server           = flag.String("server", "", "the database server")
	user             = flag.String("user", "", "the database user")
	password         = flag.String("password", "", "the database password")
	port        *int = flag.Int("port", 1433, "the database port")
	database         = flag.String("database", "", "db_name")
	filepath         = flag.String("filepath", "", "sql filepath")
	dialTimeout      = flag.Int("dialTimeout", 5, "the dial timeout")
)

func main() {

	flag.Parse() // parse the command line args

	if *debug {
		fmt.Printf(" password:%s\n", *password)
		fmt.Printf(" port:%d\n", *port)
		fmt.Printf(" server:%s\n", *server)
		fmt.Printf(" user:%s\n", *user)
		fmt.Printf(" database:%s\n", *database)
	}

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s;port=%d;dial timeout=%d",
		*server, *user, *password, *database, *port, *dialTimeout)

	if *debug {
		fmt.Printf(" connString:%s\n", connString)
	}
	db, err := sql.Open("mssql", connString)

	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Cannot connect: ", err.Error())
		return
	}
	defer db.Close()

	if *debug {
		fmt.Println(*filepath)
	}

	dat, err := ioutil.ReadFile(*filepath)
	check(err)

	if *debug {
		log.Println(string(dat))
	}

	err = exec(db, string(dat))
	if err != nil {
		fmt.Println(err)
	}
}

func exec(db *sql.DB, cmd string) error {
	rows, err := db.Query(cmd)
	if err != nil {
		if *debug {
			log.Println("error from db.Query")
			log.Println(err)
		}
		return err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		if *debug {
			log.Println("error from rows.Columns")
		}
		return err
	}
	if cols == nil {
		return nil
	}
	vals := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		vals[i] = new(interface{})
		if i != 0 {
			fmt.Print("\t")
		}
		fmt.Print(cols[i])
	}
	fmt.Println()
	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			if *debug {
				log.Println("error from rows.Scan")
			}
			fmt.Println(err)
			continue
		}
		for i := 0; i < len(vals); i++ {
			if i != 0 {
				fmt.Print("\t")
			}
			printValue(vals[i].(*interface{}))
		}
		fmt.Println()

	}
	if rows.Err() != nil {
		if *debug {
			log.Println("error from rows.Err")
		}
		return rows.Err()
	}
	return nil
}

func printValue(pval *interface{}) {
	switch v := (*pval).(type) {
	case nil:
		fmt.Print("NULL")
	case bool:
		if v {
			fmt.Print("1")
		} else {
			fmt.Print("0")
		}
	case []byte:
		fmt.Print(string(v))
	case time.Time:
		fmt.Print(v.Format("20060102 15:04:05.999"))
	default:
		fmt.Print(v)
	}
}
