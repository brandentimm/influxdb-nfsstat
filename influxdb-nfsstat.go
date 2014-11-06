package main

import (
	"bufio"
	"fmt"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/influxdb"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

type OpCount []int64

type IoCountStore struct {
	getattr     metrics.Gauge
	setattr     metrics.Gauge
	lookup      metrics.Gauge
	access      metrics.Gauge
	readlink    metrics.Gauge
	read        metrics.Gauge
	write       metrics.Gauge
	create      metrics.Gauge
	mkdir       metrics.Gauge
	symlink     metrics.Gauge
	mknod       metrics.Gauge
	remove      metrics.Gauge
	rmdir       metrics.Gauge
	rename      metrics.Gauge
	link        metrics.Gauge
	readdir     metrics.Gauge
	readdirplus metrics.Gauge
	fsstat      metrics.Gauge
	fsinfo      metrics.Gauge
	pathconf    metrics.Gauge
	commit      metrics.Gauge
}

func NewIoCountStore() *IoCountStore {
	store := &IoCountStore{
		getattr:     metrics.NewGauge(),
		setattr:     metrics.NewGauge(),
		lookup:      metrics.NewGauge(),
		access:      metrics.NewGauge(),
		readlink:    metrics.NewGauge(),
		read:        metrics.NewGauge(),
		write:       metrics.NewGauge(),
		create:      metrics.NewGauge(),
		mkdir:       metrics.NewGauge(),
		symlink:     metrics.NewGauge(),
		mknod:       metrics.NewGauge(),
		remove:      metrics.NewGauge(),
		rmdir:       metrics.NewGauge(),
		rename:      metrics.NewGauge(),
		link:        metrics.NewGauge(),
		readdir:     metrics.NewGauge(),
		readdirplus: metrics.NewGauge(),
		fsstat:      metrics.NewGauge(),
		fsinfo:      metrics.NewGauge(),
		pathconf:    metrics.NewGauge(),
		commit:      metrics.NewGauge(),
	}
	metrics.Register("bart.glbrc.org.nfs.io.getattr", store.getattr)
	metrics.Register("bart.glbrc.org.nfs.io.setattr", store.setattr)
	metrics.Register("bart.glbrc.org.nfs.io.lookup", store.lookup)
	metrics.Register("bart.glbrc.org.nfs.io.access", store.access)
	metrics.Register("bart.glbrc.org.nfs.io.readlink", store.readlink)
	metrics.Register("bart.glbrc.org.nfs.io.read", store.read)
	metrics.Register("bart.glbrc.org.nfs.io.write", store.write)
	metrics.Register("bart.glbrc.org.nfs.io.create", store.create)
	metrics.Register("bart.glbrc.org.nfs.io.mkdir", store.mkdir)
	metrics.Register("bart.glbrc.org.nfs.io.symlink", store.symlink)
	metrics.Register("bart.glbrc.org.nfs.io.mknod", store.mknod)
	metrics.Register("bart.glbrc.org.nfs.io.remove", store.remove)
	metrics.Register("bart.glbrc.org.nfs.io.rmdir", store.rmdir)
	metrics.Register("bart.glbrc.org.nfs.io.rename", store.rename)
	metrics.Register("bart.glbrc.org.nfs.io.link", store.link)
	metrics.Register("bart.glbrc.org.nfs.io.readdir", store.readdir)
	metrics.Register("bart.glbrc.org.nfs.io.readdirplus", store.readdirplus)
	metrics.Register("bart.glbrc.org.nfs.io.fsstat", store.fsstat)
	metrics.Register("bart.glbrc.org.nfs.io.fsinfo", store.fsinfo)
	metrics.Register("bart.glbrc.org.nfs.io.pathconf", store.pathconf)
	metrics.Register("bart.glbrc.org.nfs.io.commit", store.commit)
	return store
}

func readAndUpdate(opStore *IoCountStore, ready chan bool) {
	start := time.Now()
	const nfsStatFile string = "/proc/net/rpc/nfs"

	data, err := ioutil.ReadFile(nfsStatFile)
	if err != nil {
		panic(fmt.Sprintf("Could not open %s.", nfsStatFile))
	}
	nfsStats := fmt.Sprintf("%s", string(data))
	var iopCount OpCount

	lineScanner := bufio.NewScanner(strings.NewReader(nfsStats))
	lineScanner.Split(bufio.ScanLines)
	for lineScanner.Scan() {
		wordScanner := bufio.NewScanner(strings.NewReader(lineScanner.Text()))
		wordScanner.Split(bufio.ScanWords)
		wordScanner.Scan()

		if wordScanner.Text() == "proc3" {
			for wordScanner.Scan() {
				count, err := strconv.ParseInt(wordScanner.Text(), 10, 64)
				if err != nil {
					panic(fmt.Sprintf("Cannot convert string to uint64, %s", err))
				}
				iopCount = append(iopCount, count)
			}
			fmt.Fprintf(os.Stdout, "%v", iopCount)

		}
	}
	if len(iopCount) == 0 {
		fmt.Fprintf(os.Stderr, "No NFS v3 mounts, exiting.")
		os.Exit(0)
	}
	fmt.Fprintf(os.Stdout, "%v\n", iopCount)

	opStore.getattr.Update(iopCount[1])
	opStore.setattr.Update(iopCount[2])
	opStore.lookup.Update(iopCount[3])
	opStore.access.Update(iopCount[4])
	opStore.readlink.Update(iopCount[5])
	opStore.read.Update(iopCount[6])
	opStore.write.Update(iopCount[7])
	opStore.create.Update(iopCount[8])
	opStore.mkdir.Update(iopCount[9])
	opStore.symlink.Update(iopCount[10])
	opStore.mknod.Update(iopCount[11])
	opStore.remove.Update(iopCount[12])
	opStore.rmdir.Update(iopCount[13])
	opStore.rename.Update(iopCount[14])
	opStore.link.Update(iopCount[15])
	opStore.readdir.Update(iopCount[16])
	opStore.readdirplus.Update(iopCount[17])
	opStore.fsstat.Update(iopCount[18])
	opStore.fsinfo.Update(iopCount[19])
	opStore.pathconf.Update(iopCount[20])
	opStore.commit.Update(iopCount[21])
	go influxdb.Influxdb(metrics.DefaultRegistry, 10e9, &influxdb.Config{
		Host:     "127.0.0.1:49153",
		Database: "testdb",
		Username: "root",
		Password: "root",
	})
	elapsed := time.Since(start)
	fmt.Fprintf(os.Stdout, "Read and Update took d%", int64(elapsed))
	ready <- true
}

func main() {
	ready := make(chan bool, 1)
	ready <- true
	fmt.Fprintf(os.Stdout, "%v", ready)
	opStore := NewIoCountStore()
	for {
		time.Sleep(time.Second * 5)
		<-ready
		fmt.Fprintf(os.Stdout, "Channel is ready!")
		go readAndUpdate(opStore, ready)
	}
}
