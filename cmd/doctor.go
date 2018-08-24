// Copyright © 2018 Hong Bin <hongbin@actionsky.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"sort"
	"strings"
	// "strconv"
	// "strings"
	"time"
)

// doctorCmd represents the doctor command
var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "mysql error log anaylze ",
	Long:  `anaylze mysql error log about semaphore crash`,
	Run: func(cmd *cobra.Command, args []string) {

		anaylzeErrorLog(file)
	},
}
var file string

func init() {
	RootCmd.AddCommand(doctorCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	doctorCmd.Flags().StringVarP(&file, "file", "f", "", "MySQL error log file")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// doctorCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func anaylzeErrorLog(file string) {

	var restartTime, SemaphoreTime, writerState []string
	var version, waitKey, writerKey string

	restarts := int(0)
	Semaphores := int(0)
	waitPoint := map[string][]string{}
	writer := map[string][]string{}
	lastWriteLock := map[string][]string{}
	lastReadLock := map[string][]string{}
	// f, err := ioutil.ReadFile(file)
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	// defer f.Close()
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "Version:") {
			version = strings.Fields(scanner.Text())[1]
		}
		if strings.Contains(scanner.Text(), "ready for connections") {
			restarts++
			t := formatTime(strings.Fields(scanner.Text())[0])
			restartTime = append(restartTime, t)
		}

		if strings.Contains(scanner.Text(), "Semaphore wait has lasted > 600 seconds") {
			Semaphores++
			t := formatTime(strings.Fields(scanner.Text())[0])
			SemaphoreTime = append(SemaphoreTime, t)
		}

		// --Thread 140202719565568 has waited at ha_innodb.cc line 14791 for 244.00 seconds the semaphore:
		if strings.Contains(scanner.Text(), "has waited at") {

			str := strings.Fields(scanner.Text())
			// str[5]: ha_innodb.cc  str[7]: 14791 str[1]: 140202719565568
			// if s, _ := strconv.ParseFloat(str[9], 32); s > 900 {
			waitKey = fmt.Sprintf("%s:%s", str[5], str[7])
			waitPoint[waitKey] = append(waitPoint[waitKey], str[1])
		}
		// }

		//a writer (thread id 140617682765568) has reserved it in mode  exclusive
		if strings.Contains(scanner.Text(), "has reserved it in mode") {
			writerKey = strings.Trim(strings.Fields(scanner.Text())[4], ")")
			writer[writerKey] = append(writer[writerKey], waitKey)
		}
		// for k := range writer {

		// if strings.Contains(scanner.Text(), fmt.Sprintf("Thread %s", )) {
		// 	delete(writer, k)
		// }
		if strings.Contains(scanner.Text(), fmt.Sprintf("OS thread handle %s", writerKey)) {

			writerState = append(writerState, scanner.Text())

		}

		// }

		//Last time read locked in file row0purge.cc line 861
		if strings.Contains(scanner.Text(), "Last time read locked") {
			str := strings.Fields(scanner.Text())
			lastReadLock[writerKey] = append(lastReadLock[writerKey], fmt.Sprintf("%s:%s", str[6], str[8]))
		}

		//Last time write locked in file /usr/local/mysql-install/storage/innobase/dict/dict0stats.cc line 2366
		if strings.Contains(scanner.Text(), "Last time write locked") {
			str := strings.Split(scanner.Text(), "/")
			lastWriteLock[writerKey] = append(lastWriteLock[writerKey], strings.Replace(str[len(str)-1], " line ", ":", 1))
		}

	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	fmt.Printf("MySQL Server Version: %s\n", version)
	fmt.Print("\n********** MySQL service start count **********\n")
	fmt.Printf("MySQL Semaphore crash -> %v times %q\n", Semaphores, SemaphoreTime)
	fmt.Printf("  MySQL Service start -> %v times %q\n", restarts, restartTime)

	fmt.Print("\n********** Which thread waited lock **********")
	for k, v := range waitPoint {
		fmt.Printf("\n%20s -> %3v  %v\n", k, len(v), unique(v))
	}

	fmt.Print("\n********** Which writer threads block at **********")
	for k, v := range writer {
		fmt.Printf("\n%20s -> %3v  %v\n", k, len(v), unique(v))
	}

	fmt.Print("\n********** These writer threads trx state **********\n")
	for _, v := range writerState {
		fmt.Println(v)
	}

	fmt.Print("\n********** These writer threads at last time reads locked **********")
	for k, v := range lastReadLock {
		fmt.Printf("\n%20s -> %3v  %v\n", k, len(v), unique(v))
	}

	fmt.Print("\n********** These writer threads at last time write locked **********")
	for k, v := range lastWriteLock {
		fmt.Printf("\n%20s -> %3v  %v\n", k, len(v), unique(v))
	}

}

func sortMapByValue(m map[string]uint64) string {
	type kv struct {
		Key   string
		Value uint64
	}
	b := new(bytes.Buffer)
	var ss []kv
	for k, v := range m {
		ss = append(ss, kv{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})

	for _, kv := range ss {
		fmt.Fprintf(b, "%s:(%v) ", kv.Key, kv.Value)
	}
	return b.String()
}

func unique(intSlice []string) string {
	counter := make(map[string]uint64)
	for _, row := range intSlice {
		counter[row]++
	}
	return sortMapByValue(counter)
}

func formatTime(s string) string {
	layoutIn := "2006-01-02T15:04:05.000000+08:00"
	layoutOut := "2006-01-02 15:04:05"
	t, err := time.Parse(layoutIn, s)
	if err != nil {
		fmt.Println(err)
	}
	return t.Format(layoutOut)
}
