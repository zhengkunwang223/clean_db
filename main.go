package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const (
	dbDir      = "/opt/1panel/apps/openresty/openresty/1pwaf/data/db/sites"
	mainDbPath = "/opt/1panel/apps/openresty/openresty/1pwaf/data/db/req_log.db"
)

func main() {
	// 处理主数据库
	err := processMainDatabase(mainDbPath)
	if err != nil {
		log.Printf("处理主数据库时出错: %v", err)
	}

	// 处理 sites 目录下的数据库
	err = filepath.Walk(dbDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Base(path) == "site_req_logs.db" {
			err := processSiteDatabase(path)
			if err != nil {
				log.Printf("处理站点数据库 %s 时出错: %v", path, err)
			}
		}
		return nil
	})

	if err != nil {
		log.Fatalf("遍历目录时出错: %v", err)
	}
}

func processMainDatabase(dbPath string) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("无法打开数据库: %v", err)
	}
	defer db.Close()

	// 删除 req_logs 表中一半的历史数据
	result, err := db.Exec(`
		DELETE FROM req_logs
		WHERE rowid IN (
			SELECT rowid FROM req_logs
			ORDER BY localtime ASC
			LIMIT (SELECT COUNT(*) / 2 FROM req_logs)
		)
	`)
	if err != nil {
		return fmt.Errorf("删除 req_logs 数据时出错: %v", err)
	}
	rowsDeleted, _ := result.RowsAffected()

	// 优化数据库
	_, err = db.Exec("VACUUM")
	if err != nil {
		return fmt.Errorf("优化数据库时出错: %v", err)
	}

	fmt.Printf("主数据库 %s: 已成功删除 %d 条 req_logs 记录并优化数据库。\n", dbPath, rowsDeleted)
	return nil
}

func processSiteDatabase(dbPath string) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("无法打开数据库: %v", err)
	}
	defer db.Close()

	// 删除 site_req_logs 表中一半的历史数据
	result, err := db.Exec(`
		DELETE FROM site_req_logs
		WHERE rowid IN (
			SELECT rowid FROM site_req_logs
			ORDER BY localtime ASC
			LIMIT (SELECT COUNT(*) / 2 FROM site_req_logs)
		)
	`)
	if err != nil {
		return fmt.Errorf("删除 site_req_logs 数据时出错: %v", err)
	}
	rowsDeleted, _ := result.RowsAffected()

	// 优化数据库
	_, err = db.Exec("VACUUM")
	if err != nil {
		return fmt.Errorf("优化数据库时出错: %v", err)
	}

	fmt.Printf("站点数据库 %s: 已成功删除 %d 条 site_req_logs 记录并优化数据库。\n", dbPath, rowsDeleted)
	return nil
}
