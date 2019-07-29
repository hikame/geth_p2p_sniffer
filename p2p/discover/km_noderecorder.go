package discover

import (
	"bytes"
	"database/sql"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // drive of sqlite3
)

// SqliteDB This is used for sqlite operation
type SqliteDB struct {
	dbMutex  sync.Mutex
	database *sql.DB
}

var sdb SqliteDB

func checkErr(err error) {
	if err != nil {
		if sdb.database != nil {
			sdb.database.Close()
		}
		panic(err)
	}
}

func (sdb *SqliteDB) checkDB() {
	if sdb.database != nil {
		return
	}
	sdb.dbMutex.Lock()
	if sdb.database == nil {
		db, err := sql.Open("sqlite3", "./nodes_info.db")
		checkErr(err)
		sdb.database = db
		ctstr := "CREATE TABLE `nodeinfo` (`ID` BLOB, `SHA` BLOB, `IP` TEXT, `TCP` INTEGER, `UDP` INTEGER, `BONDED` INTEGER, `TimeStmp` INTEGER)"
		_, err = sdb.database.Exec(ctstr)
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				checkErr(err)
			} /*else {
				log.Info("[DEBG] Table \"nodeinfo\" already exists.")
			} */
		}
		// defer sdb.database.Close() //TODO check this out!
	}
	sdb.dbMutex.Unlock()
}

func createNodeTable(tname string) {
	sdb.checkDB()

	var buf bytes.Buffer
	buf.WriteString("create table node_")
	buf.WriteString(tname)
	buf.WriteString(" (NODE BLOB, TimeStmp INTEGER)")
	sdb.dbMutex.Lock()
	_, err := sdb.database.Exec(buf.String())
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			checkErr(err)
		} /* else {
			log.Info("[DEBG] Table already exists.", "Node", tname)
		}*/
	}
	sdb.dbMutex.Unlock()
}

func insertIntoTable(table string, node string, time int64) {
	sdb.checkDB()
	var buf bytes.Buffer
	// buf.WriteString("select count(*) from node_")
	buf.WriteString("select * from node_")
	buf.WriteString(table)
	buf.WriteString(" where node=?")
	// buf.WriteString(node)
	// buf.WriteString("\"")
	qrstr := buf.String()
	buf.Reset()

	buf.WriteString("insert into node_")
	buf.WriteString(table)
	buf.WriteString("(NODE, TimeStmp) values(?,?)")
	instr := buf.String()
	buf.Reset()

	buf.WriteString("update node_")
	buf.WriteString(table)
	buf.WriteString(" set TimeStmp=? where node=?")
	udstr := buf.String()

	sdb.dbMutex.Lock()
	rows, err := sdb.database.Query(qrstr, node)
	checkErr(err)

	if rows.Next() {
		// we already have this record, update the time stmp
		// log.Info("[TEST] [NEIB] We already have this record, update the time stmp")
		rows.Close()
		stmt, err := sdb.database.Prepare(udstr)
		checkErr(err)
		_, err = stmt.Exec(time, node)
		checkErr(err)
	} else {
		// we do not have this record, do insert
		rows.Close()
		stmt, err := sdb.database.Prepare(instr)
		checkErr(err)
		_, err = stmt.Exec(node, time)
		checkErr(err)
	}
	sdb.dbMutex.Unlock()
}

const (
	// bqrstr := "select * from nodeinfo where ID=\"?\" and SHA=\"?\" and IP=\"?\" and TCP=? and UDP=?")
	bqrstr string = "select * from nodeinfo where ID=?"
	binstr string = "insert into nodeinfo (ID, SHA, IP, TCP, UDP, BONDED, TimeStmp) values(?, ?, ?, ?, ?, ?, ?)"
	budstr string = "update nodeinfo set SHA=?, BONDED=?, TimeStmp=? where ID=?"
)

func insertBondInfo(id string, sha string, ip string, tcp uint16, udp uint16, bonded bool) {
	sdb.checkDB()
	timestmp := time.Now().Unix()
	sdb.dbMutex.Lock()
	rows, err := sdb.database.Query(bqrstr, id)
	checkErr(err)
	// var count int
	// rows.Scan(&count)
	// log.Info("[TEST] [BOND]", "Count", count)
	if rows.Next() {
		// log.Info("[TEST] [BOND] We already have this record, update the time stmp")
		// we already have this record, update the time stmp
		rows.Close()
		stmt, err := sdb.database.Prepare(budstr)
		checkErr(err)
		_, err = stmt.Exec(sha, bonded, timestmp, id)
		checkErr(err)
	} else {
		// we do not have this record, do insert
		rows.Close()
		stmt, err := sdb.database.Prepare(binstr)
		checkErr(err)
		_, err = stmt.Exec(id, sha, ip, tcp, udp, bonded, timestmp)
		checkErr(err)
	}
	sdb.dbMutex.Unlock()
}
