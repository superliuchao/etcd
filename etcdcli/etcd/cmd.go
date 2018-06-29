package etcd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
	"strings"
	"errors"
	"context"
	"log"
	"os"
	etcdv2 "github.com/coreos/etcd/client"
)

type  OnChangeCallback func(action string, path string, value string) bool //return true exit watch



type ClientAPIs  interface {
	Set(key string, value string, ttl int64, swapValue string, swapIndex int64) error
	SetDir(key string, ttl int64) error
	Update(key string, value string, ttl int64) error
	UpdateDir(key string, value string) error
	RM(key string, idDir bool, recursive bool,  preValue string, preIndex int64) error
	RMDir(key string) error
	Get(key string) (string, error)
	List(path string, recursive bool) ([] string)
	MK(key string, value string, ttl int64) error
	MKDir(key string, ttl int64) error
	Watch(key string, recursive bool, onChange OnChangeCallback) (error)

}

type Client struct {
	sync.Mutex
	etcdKeysApi etcdv2.KeysAPI
	timeout time.Duration

	closed  bool
	cancel  context.CancelFunc
	ctx context.Context

	client etcdv2.Client

}

/*
func NewClientAPIs() {

}
*/

func printResponseKey(resp *etcdv2.Response, format string) {
	// Format the result.
	switch format {
	case "simple":
		if resp.Action != "delete" {
			fmt.Println(resp.Node.Value)
		} else {
			fmt.Println("PrevNode.Value:", resp.PrevNode.Value)
		}
	case "extended":
		// Extended prints in a rfc2822 style format
		fmt.Println("Key:", resp.Node.Key)
		fmt.Println("Created-Index:", resp.Node.CreatedIndex)
		fmt.Println("Modified-Index:", resp.Node.ModifiedIndex)

		if resp.PrevNode != nil {
			fmt.Println("PrevNode.Value:", resp.PrevNode.Value)
		}

		fmt.Println("TTL:", resp.Node.TTL)
		fmt.Println("Index:", resp.Index)
		if resp.Action != "delete" {
			fmt.Println("")
			fmt.Println(resp.Node.Value)
		}
	case "json":
		b, err := json.Marshal(resp)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(b))
	default:
		fmt.Fprintln(os.Stderr, "Unsupported output format:", format)
	}
}


func NewClient(ips []string, auth string, timeout time.Duration) (*Client, error) {
	if len(ips) == 0 {
		return nil, errors.New("endpoint is empty")
	}

	for i, ip := range ips {
		if ip != "" && !strings.HasPrefix(ip, "http://") {
			ips[i] = "http://" + ip
		}
	}

	if (timeout <= 0) {
		timeout = time.Second * 5
		//return nil, errors.New("timeout is le 0")
	}


	config := etcdv2.Config{
		Endpoints: ips,
		Transport: etcdv2.DefaultTransport,
		HeaderTimeoutPerRequest: timeout * time.Second,
	}

	if auth != "" {
		split := strings.SplitN(auth, ":", 2)
		if len(split) != 2 || split[0] == "" {
			return nil, errors.New("invalid auth")
		}
		config.Username = split[0]
		config.Password = split[1]
	}

	c, err := etcdv2.New(config)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("etcd new %v", config))
	}

	fmt.Printf("config:%+v\n", config)
	client := &Client{
		etcdKeysApi: etcdv2.NewKeysAPI(c), timeout: timeout, client: c,
	}

	client.ctx, client.cancel = context.WithCancel(context.Background())
	//cntx, cancle := client.newContextWithTimeout()
	//cancle = cancle
	//resp, resperr := client.etcdKeysApi.Set(cntx, "/temp", "123", &etcdv2.SetOptions{})
	//fmt.Printf("temp: [%v][%v]\n", resp, resperr)
	return client, nil
}


func (c *Client) newContextWithTimeout() (context.Context, context.CancelFunc){
	//return context.WithTimeout(context.Background(), c.timeout)
	return context.WithTimeout(c.ctx, c.timeout)
}

func (c *Client) Set(key string, value string, ttl int64, prevValue string, prevIndex int64) error {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	resp, err := c.etcdKeysApi.Set(ctx, key, value, &etcdv2.SetOptions{TTL: time.Duration(ttl) * time.Second, PrevIndex: uint64(prevIndex), PrevValue: prevValue})
	cancel()
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return err
	}
	printResponseKey(resp, "extended")
	return nil
}

func (c *Client) SetDir(key string, ttl int64) error {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	resp, err := c.etcdKeysApi.Set(ctx, key, "", &etcdv2.SetOptions{TTL: time.Duration(ttl) * time.Second, Dir: true, PrevExist: etcdv2.PrevIgnore})
	cancel()
	if err != nil {
		fmt.Printf("err %v\n", err)
		return err
	}
	printResponseKey(resp, "extended")
	return err
}

func (c *Client) Update(key string, value string, ttl int64) error {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	resp, err := c.etcdKeysApi.Set(ctx, key, value, &etcdv2.SetOptions{TTL: time.Duration(ttl) * time.Second, PrevExist: etcdv2.PrevExist})
	cancel()
	if err != nil {
		fmt.Printf("err %v\n", err)
		return err
	}
	printResponseKey(resp, "extended")
	return nil

}

func (c *Client) UpdateDir(key string, value string, ttl int64) error {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	resp, err := c.etcdKeysApi.Set(ctx, key, "", &etcdv2.SetOptions{TTL: time.Duration(ttl) * time.Second, Dir: true, PrevExist: etcdv2.PrevExist})
	cancel()
	if err != nil {
		fmt.Printf("err %v\n", err)
		return err
	}
	printResponseKey(resp, "extended")
	return nil
}

func (c *Client) RM(key string, dir bool, recursive bool,  prevValue string, prevIndex int64) error {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	resp, err := c.etcdKeysApi.Delete(ctx, key, &etcdv2.DeleteOptions{PrevIndex: uint64(prevIndex), PrevValue: prevValue, Dir: dir, Recursive: recursive})
	cancel()
	if err != nil {
		fmt.Printf("err %v\n", err)
		return err
	}
	printResponseKey(resp, "extended")
	return nil
}

func (c *Client) RMDir(key string) error {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	resp, err := c.etcdKeysApi.Delete(ctx, key, &etcdv2.DeleteOptions{Dir: true})
	cancel()
	if err != nil {
		fmt.Printf("err %v\n", err)
		return err
	}
	printResponseKey(resp, "extended")
	return nil
}

func (c *Client) Get(key string) (string, error) {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	resp, err := c.etcdKeysApi.Get(ctx, key, &etcdv2.GetOptions{Sort: true, Quorum: true})
	cancel()
	if err != nil {
		fmt.Printf("err %v\n", err)
		return "", err
	}
	if resp.Node.Dir {
		fmt.Printf("%s", fmt.Sprintf("%s: is a directory", resp.Node.Key))
		return "", errors.New(fmt.Sprintf("%s: is a directory", resp.Node.Key))
	}
	printResponseKey(resp, "extended")
	return resp.Node.Value, nil
}

func (c *Client) List(path string, recursive bool) ([] string, error) {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	resp, err := c.etcdKeysApi.Get(ctx, path, &etcdv2.GetOptions{Sort: true, Quorum: true, Recursive: recursive})
	cancel()
	switch {
	case err != nil:
		fmt.Printf("etcd list node %s failed: %s", path, err)
		return nil, err
	case !resp.Node.Dir:
		log.Printf("etcd list node %s failed: not a dir", path)
		return nil, err
	default:
		/*
		var paths []string
		for _, node := range resp.Node.Nodes {
			paths = append(paths, node.Key)
		}
		*/

		printResponseKey(resp, "extended")
		return nodesToStringSlice(resp.Node.Nodes), nil
	}
}

func (c *Client) MK(key string, value string, ttl int64, inorder bool) error {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	var err error
	var resp *etcdv2.Response

	if !inorder {
		resp, err = c.etcdKeysApi.Set(ctx, key, value, &etcdv2.SetOptions{TTL: time.Duration(ttl) * time.Second, PrevExist: etcdv2.PrevNoExist})
		if (err != nil) {

		} else {
			printResponseKey(resp, "extended")
		}

	} else {
		resp, err = c.etcdKeysApi.CreateInOrder(ctx, key, value, &etcdv2.CreateInOrderOptions{TTL: time.Duration(ttl) * time.Second})
		if (err != nil) {

		} else {
			printResponseKey(resp, "extended")
		}
	}
	cancel()
	if err != nil {
		fmt.Printf("err %v\n", err)
		return err
	}
	return nil
}

func (c *Client) MKDir(key string, ttl int64) error {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	resp, err := c.etcdKeysApi.Set(ctx, key, "", &etcdv2.SetOptions{TTL: time.Duration(ttl) * time.Second, Dir: true, PrevExist: etcdv2.PrevNoExist})
	cancel()
	if err != nil {
		fmt.Printf("err %v\n", err)
		return err
	}
	printResponseKey(resp, "extended")
	return nil
}

func (c *Client) GetResonse(key string, sort bool, recursive bool) (*etcdv2.Response, error) {
	c.Lock()
	defer c.Unlock()
	ctx, cancel := c.newContextWithTimeout()
	resp, err := c.etcdKeysApi.Get(ctx, key, &etcdv2.GetOptions{Sort: sort, Quorum: true, Recursive: recursive})
	cancel()
	if err != nil {
		fmt.Printf("err %v\n", err)
		return resp, err
	}
	printResponseKey(resp, "extended")
	return resp, err
}

//can not return until err from server
func (c *Client) Watch(key string, recursive bool, onChange OnChangeCallback) (error) {
	//c.Lock()
	//defer c.Unlock()
	//exit := make(chan struct{},1)
	afterIndex := uint64(0)
	watcher := c.etcdKeysApi.Watcher(key, &etcdv2.WatcherOptions{AfterIndex: afterIndex, Recursive: recursive})
	for {
		//resp, err := watcher.Next(context.Background())
		resp, err := watcher.Next(c.ctx)
		if err != nil {
			if shouldIgnoreError(err) {
				continue
			}

			return err
		}

		afterIndex = resp.Index
		if (true == onChange(resp.Action, resp.Node.Key, resp.Node.Value)) {
			return err
		}

	}
}

func (c *Client) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}

	c.closed = true
	c.cancel()
	return nil
}

func shouldIgnoreError(err error) bool {
	switch err := err.(type) {
	default:
		return false
	case *etcdv2.Error:
		return err.Code == etcdv2.ErrorCodeEventIndexCleared
	}
}

/*
	ErrorCodeKeyNotFound  = 100
	ErrorCodeTestFailed   = 101
	ErrorCodeNotFile      = 102
	ErrorCodeNotDir       = 104
	ErrorCodeNodeExist    = 105
	ErrorCodeRootROnly    = 107
	ErrorCodeDirNotEmpty  = 108
	ErrorCodeUnauthorized = 110
*/


func IsEtcdNotDirEmpty(err error) bool {
	return isEtcdErrorNum(err, etcdv2.ErrorCodeDirNotEmpty)
}
func IsEtcdNotDir(err error) bool {
	return isEtcdErrorNum(err, etcdv2.ErrorCodeNotDir)
}

func IsEtcdNotFile(err error) bool {
	return isEtcdErrorNum(err, etcdv2.ErrorCodeNotFile)
}

func IsEtcdNotFound(err error) bool {
	return isEtcdErrorNum(err, etcdv2.ErrorCodeKeyNotFound)
}


func IsEtcdNodeExist(err error) bool {
	return isEtcdErrorNum(err, etcdv2.ErrorCodeNodeExist)
}


func IsEtcdTestFailed(err error) bool {
	return isEtcdErrorNum(err, etcdv2.ErrorCodeTestFailed)
}


func IsEtcdWatchExpired(err error) bool {
	return isEtcdErrorNum(err, etcdv2.ErrorCodeEventIndexCleared)
}


func IsEtcdUnreachable(err error) bool {
	return err == etcdv2.ErrClusterUnavailable
}


func isEtcdErrorNum(err error, errorCode int) bool {
	if err != nil {
		if etcdError, ok := err.(etcdv2.Error); ok {
			return etcdError.Code == errorCode
		}
		// NOTE: There are other error types returned
	}
	return false
}

func nodesToStringSlice(nodes etcdv2.Nodes) []string {
	var keys []string

	for _, node := range nodes {
		keys = append(keys, node.Key)

		for _, key := range nodesToStringSlice(node.Nodes) {
			keys = append(keys, key)
		}
	}

	return keys
}


func GetEtcdVersion(host string) (string, error) {
	response, err := http.Get(host + "/version")
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unsuccessful response from etcd server %q: %v", host, err)
	}
	versionBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	return string(versionBytes), nil
}

type etcdHealth struct {
	Health string `json:"health"`
}

func EtcdHealthCheck(data []byte) error {
	obj := etcdHealth{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	if obj.Health != "true" {
		return fmt.Errorf("Unhealthy status: %s", obj.Health)
	}
	return nil
}

