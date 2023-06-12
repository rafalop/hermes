package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

var contentParser *ContentParser

var (
	frontendDir string
	viewDir     string
)

func init() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		logrus.Fatal(err)
	}

	flag.StringVar(&frontendDir, "frontend_dir", homeDir+string("/frontend"), "The path of frontend directory")
	flag.StringVar(&viewDir, "view_dir", homeDir+string("/view"), "The path of view directory")
	flag.Usage = usage
}

func usage() {
	fmt.Println("Usage: webserver [frontend_dir] [view_dir]")
	flag.PrintDefaults()
}

func main() {
	router := gin.Default()

	flag.Parse()

	contentParser = NewContentParser(viewDir)
	router.LoadHTMLGlob(frontendDir + string("/*.html"))
	router.Static("/assets", frontendDir+string("/assets"))
	router.Static("/css", frontendDir+string("/css"))

	page := router.Group("/")
	{
		page.GET("", func(ctx *gin.Context) {
			ctx.HTML(http.StatusOK, "index.html", nil)
		})
	}

	api := router.Group("/api")
	{
		api.GET("/routines", func(ctx *gin.Context) {
			ctx.ProtoBuf(http.StatusOK, contentParser.GetRoutines())
		})
	}

	cpu := router.Group("/cpu")
	{
		cpu.GET("/cpu_profile", func(ctx *gin.Context) {
			path := filepath.Join(viewDir, "cpu_profile", "overview")
			ctx.File(path)
		})
		cpu.GET("/cpu_profile/:timestamp", func(ctx *gin.Context) {
			timestamp := ctx.Param("timestamp")
			path := filepath.Join(viewDir, "cpu_profile", timestamp, "overall_cpu.stack.json")
			ctx.File(path)
		})
	}

	mem := router.Group("/memory")
	{
		mem.GET("/memory_ebpf", func(ctx *gin.Context) {
			path := filepath.Join(viewDir, "memory_ebpf", "overview")
			ctx.File(path)
		})
		mem.GET("/memory_ebpf/:timestamp", func(ctx *gin.Context) {
			timestamp := ctx.Param("timestamp")
			path := filepath.Join(viewDir, "memory_ebpf", timestamp, "slab.stack.json")
			ctx.File(path)
		})
	}

	router.NoRoute(func(ctx *gin.Context) {
		ctx.JSON(http.StatusNotFound, gin.H{})
	})
	router.Run()
}
