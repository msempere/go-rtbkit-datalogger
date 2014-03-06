package main

import (
    l4g "code.google.com/p/log4go"
    "encoding/json"
    "fmt"
    zmq "github.com/alecthomas/gozmq"
    "github.com/bitly/go-simplejson"
    "launchpad.net/gozk/zookeeper"
    "strconv"
)

type S3Logger struct {
    backup_dir          string
    file_name           string
    lines               int
    zookeeper_host      string
    zookeeper_port      int
    log                 l4g.Logger
    last_file           string
    subscriber          *zmq.Socket
    string_to_subscribe string
}

func NewS3Logger(backup_dir string, file_name string, zookeeper_host string, zookeeper_port int, string_to_subscribe string, lines int) *S3Logger {
    logger := new(S3Logger)
    logger.backup_dir = backup_dir
    logger.file_name = file_name
    logger.lines = lines
    logger.zookeeper_host = zookeeper_host
    logger.string_to_subscribe = string_to_subscribe
    logger.zookeeper_port = zookeeper_port
    logger.log = make(l4g.Logger)
    flw := l4g.NewFileLogWriter(backup_dir+logger.file_name, false)
    flw.SetFormat("[%D %T]\n\t%M")
    flw.SetRotate(true)
    flw.SetRotateLines(logger.lines)

    logger.log.AddFilter(logger.backup_dir+logger.file_name, l4g.FINE, flw)

    return logger
}

func (l S3Logger) Write(what string) {
    l.log.Info("", what)
}

func (l S3Logger) getServiceURI(what string) string {

    zk, session, err := zookeeper.Dial(l.zookeeper_host+":"+strconv.Itoa(l.zookeeper_port), 5e9)
    if err != nil {
        fmt.Println("Couldn't connect: " + err.Error())
        return ""
    }
    defer zk.Close()
    event := <-session

    if event.State != zookeeper.STATE_CONNECTED {
        fmt.Println("Couldn't connect")
        return ""
    } else {
        fmt.Println("Connected to Zookeeper")
    }
    exists, err := zk.Exists(what)

    if err != nil {
        fmt.Println("Couldn't get path: " + err.Error())
        return ""
    } else {
        if exists == nil {
            fmt.Println("Path doesn't exist")
        } else {
            data, _, _ := zk.Get(what)

            var dat map[string]interface{}

            if err := json.Unmarshal([]byte(data), &dat); err != nil {
                fmt.Println("Couldn't decode router path")
                return ""
            }

            path := "/rtb-test/" + dat["servicePath"].(string) + "/logger/tcp"
            data, _, _ = zk.Get(path)

            data_json, _ := simplejson.NewJson([]byte(data))
            uri, err := data_json.GetIndex(1).Get("zmqConnectUri").String()

            if err != nil {
                fmt.Println("Couldn't get zmqConnectUri")
                return ""
            }
            return uri
        }
    }
    return ""
}

func (l S3Logger) Run(channel chan int) {

    context, _ := zmq.NewContext()
    subscriber, _ := context.NewSocket(zmq.SUB)
    defer subscriber.Close()
    router_uri := l.getServiceURI("/rtb-test/serviceClass/rtbRequestRouter/st2.router")

    if router_uri == "" {
        return
    }

    subscriber.Connect(router_uri)
    subscriber.SetSubscribe(l.string_to_subscribe)

    fmt.Println("Running S3Logger...")

    for {

        data, err := subscriber.Recv(0)
        if err != nil {
            fmt.Println(err)
            break
        }

        l.Write(string(data))
    }
    channel <- 1
}

func main() {
    log := NewS3Logger("/home/rtbkit/tmp/", "routerLogFile", "176.34.64.50", 2181, "", 5)
    ch := make(chan int)
    log.Run(ch)
    _ = <-ch
}
