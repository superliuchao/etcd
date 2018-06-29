
package main

import (
	"fmt"

	"etcdcli/etcdstruct"
)

type Config struct {
	Key1 SubConfig1        `etcd:"key1"`
	Key2 []SubConfig2      `etcd:"key2"`
	Key3 map[string]string `etcd:"key3"`
}


type SubConfig1 struct {
	Subkey1 string `etcd:"subkey1"`
	Subkey2 int    `etcd:"subkey2"`
}

type SubConfig2 struct {
	Subkey1 int64 `etcd:"subkey1"`
	Subkey2 bool  `etcd:"subkey2"`
}

func main() {
	var config Config

	etc, err := etcdstruct.NewClient([]string{
		"http://192.168.101.86:2379",
	}, "teststruct", &config)

	if err != nil {
		fmt.Println(err)
		return
	}

	if err := etc.Load(); err != nil {
		fmt.Println(err)
		return
	}

	key1, err := etc.Version(&config.Key1)
	if err != nil {
		fmt.Println(err)
		return
	}

	key1Subkey1, err := etc.Version(&config.Key1.Subkey1)
	if err != nil {
		fmt.Println(err)
		return
	}

	key1Subkey2, err := etc.Version(&config.Key1.Subkey2)
	if err != nil {
		fmt.Println(err)
		return
	}

	key2, err := etc.Version(&config.Key2)
	if err != nil {
		fmt.Println(err)
		return
	}

	key3, err := etc.Version(&config.Key3)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf(`Key1: %d
  - Subkey1: %d
  - Subkey2: %d
Key2: %d
Key3: %d
`,
		key1, key1Subkey1, key1Subkey2, key2, key3)
}

