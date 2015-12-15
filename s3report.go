/*

s3report - Collects today's S3 metrics and reports them to Graphite

Copyright (c) 2015 RapidLoop

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

var (
	accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsRegion = os.Getenv("AWS_REGION")
)

func main() {
	log.SetFlags(0)

	// Check env. vars.
	if len(accessKey) == 0 || len(secretKey) == 0 || len(awsRegion) == 0 {
		log.Fatal("Please set the environment variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION")
	}

	// Check command line args.
	prefixDefault := "s3." + awsRegion + "."
	prefix := flag.String("p", prefixDefault, "`prefix` for graphite metrics names")
	prev := flag.Bool("1", false, "collect yesterday's metrics rather than today's")
	addr := flag.String("g", "127.0.0.1:2003", "`graphite server` to send metrics to")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "s3report - Collects today's S3 metrics and reports them to Graphite\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	tcpAddr, err := net.ResolveTCPAddr("tcp", *addr)
	if err != nil {
		log.Fatal(err.Error())
	}

	// Create CloudWatch service
	svc := cloudwatch.New(session.New())

	// List all metrics in the AWS/S3 namespace
	params := &cloudwatch.ListMetricsInput{
		Namespace: aws.String("AWS/S3"),
	}
	resp, err := svc.ListMetrics(params)
	if err != nil {
		log.Fatal(err.Error())
	}

	// For each metric..
	buf := &bytes.Buffer{}
	for _, m := range resp.Metrics {
		// Get the bucket name and storage type
		var name, stype string
		for _, d := range m.Dimensions {
			if *d.Name == "BucketName" {
				name = *d.Value
			} else if *d.Name == "StorageType" {
				stype = strings.ToLower(*d.Value)
			}
		}
		// Get the bucket size in bytes
		if *m.MetricName == "BucketSizeBytes" {
			t, v := getBucketSize(svc, m.Dimensions, *prev)
			if t.IsZero() {
				log.Printf("bucket size not available for bucket %s", name)
			} else {
				fmt.Fprintf(buf, "%s%s.%s.size %d %d\n", *prefix, name, stype, v, t.Unix())
			}
		}
		// And the count of objects
		if *m.MetricName == "NumberOfObjects" {
			t, v := getBucketObjectCount(svc, m.Dimensions, *prev)
			if t.IsZero() {
				log.Printf("object count not available for bucket %s", name)
			} else {
				fmt.Fprintf(buf, "%s%s.objcount %d %d\n", *prefix, name, v, t.Unix())
			}
		}
	}

	if buf.Len() > 0 {
		fmt.Print(buf.String())
		fmt.Printf("sending to graphite server at %v:\n", tcpAddr)
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			log.Fatal(err)
		}
		buf.WriteTo(conn)
		conn.Close()
		fmt.Println("done.")
	} else {
		log.Println("No metrics were found for today.")
		log.Println("Try running it later in the day or run with \"-1\" flag.")
	}
}

func getBucketSize(svc *cloudwatch.CloudWatch, dims []*cloudwatch.Dimension, prev bool) (time.Time, int64) {
	t := time.Now().In(time.UTC)
	if prev {
		t = t.Add(-24 * time.Hour)
	}
	y, m, d := t.Date()
	st := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	et := time.Date(y, m, d, 0, 1, 0, 0, time.UTC)
	params := &cloudwatch.GetMetricStatisticsInput{
		StartTime:  aws.Time(st),
		EndTime:    aws.Time(et),
		Period:     aws.Int64(60),
		MetricName: aws.String("BucketSizeBytes"),
		Namespace:  aws.String("AWS/S3"),
		Statistics: []*string{
			aws.String("Average"),
		},
		Dimensions: dims,
		Unit:       aws.String("Bytes"),
	}

	return actualGet(svc, params)
}

func getBucketObjectCount(svc *cloudwatch.CloudWatch, dims []*cloudwatch.Dimension, prev bool) (time.Time, int64) {
	t := time.Now().In(time.UTC)
	if prev {
		t = t.Add(-24 * time.Hour)
	}
	y, m, d := t.Date()
	st := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	et := time.Date(y, m, d, 0, 1, 0, 0, time.UTC)
	params := &cloudwatch.GetMetricStatisticsInput{
		StartTime:  aws.Time(st),
		EndTime:    aws.Time(et),
		Period:     aws.Int64(60),
		MetricName: aws.String("NumberOfObjects"),
		Namespace:  aws.String("AWS/S3"),
		Statistics: []*string{
			aws.String("Average"),
		},
		Dimensions: dims,
		Unit:       aws.String("Count"),
	}

	return actualGet(svc, params)
}

func actualGet(svc *cloudwatch.CloudWatch, params *cloudwatch.GetMetricStatisticsInput) (time.Time, int64) {
	resp, err := svc.GetMetricStatistics(params)
	if err != nil {
		log.Fatal(err.Error())
	}
	if len(resp.Datapoints) == 0 {
		return time.Time{}, 0
	}

	return *resp.Datapoints[0].Timestamp, int64(*resp.Datapoints[0].Average)
}
