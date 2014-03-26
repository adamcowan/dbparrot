package main

var cache map[string]*MongoCmd = make(map[string]*MongoCmd)
var prep map[int]string = make(map[int]string)

func CachePrep(id int, request []byte) {
    prep[id] = string(request)
}

func CacheGet(request []byte) *MongoCmd {
    if cmd, ok := cache[string(request)]; ok {
        return cmd
    }
    return nil
}

func CachePut(id int, cmd *MongoCmd) {
    request := prep[id]
    cache[request] = cmd
}
