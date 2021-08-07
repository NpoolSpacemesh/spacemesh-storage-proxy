package task

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	log "github.com/EntropyPool/entropy-logger"
	"github.com/NpoolChia/chia-storage-proxy/db"
	"github.com/NpoolChia/chia-storage-server/chiaapi"
	apitypes "github.com/NpoolChia/chia-storage-server/types"
	"github.com/boltdb/bolt"
)

const PlotFilePrefix = "/plotfile"
const PlotFileHandle = PlotFilePrefix + "/"

func Upload(input Meta) {
	log.Infof(log.Fields{}, "try to serve file %v -> %v", input.PlotURL, input.Host)
	_, err := chiaapi.UploadChiaPlot(input.Host, "18080", apitypes.UploadPlotInput{
		PlotURL:   input.PlotURL,
		FinishURL: input.FinishURL,
		FailURL:   input.FailURL,
	})
	if err != nil {
		log.Errorf(log.Fields{}, "fail to notify new plot -> %v", input.Host)
		return
	}

	// 更新数据库
	update(input.PlotURL, TaskWait)
}

func Finsih(input Meta) {
	// 移除本地的 plot 文件
	files := strings.Split(input.PlotURL, PlotFilePrefix)
	if len(files) < 2 {
		log.Errorf(log.Fields{}, "invalid file description: %v", input.PlotURL)
		return
	}
	log.Infof(log.Fields{}, "remove finish plot file %v", files[1])
	_, err := os.Stat(filepath.Dir(files[1]))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// 已经移除
			update(input.PlotURL, TaskDone)
			return
		}
		log.Errorf(log.Fields{}, "remove finish plot file %v, error %v", files[1], err)
		return
	}

	os.RemoveAll(files[1])
	// 更新数据库
	update(input.PlotURL, TaskDone)
}

func Fail(input Meta) {
}

func update(key string, status uint8) error {
	bdb, err := db.BoltClient()
	if err != nil {
		return err
	}

	return bdb.Update(func(tx *bolt.Tx) error {
		bk := tx.Bucket(db.DefaultBucket)
		r := bk.Get([]byte(key))
		if r == nil {
			return errors.New("bolt key not exist")
		}

		// 删除原有的 key
		if err := bk.Delete([]byte(key)); err != nil {
			return err
		}

		meta := Meta{}
		if err := json.Unmarshal(r, &meta); err != nil {
			return err
		}
		meta.Status = status
		_meta, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		// 插入最新的 key
		return bk.Put([]byte(key), _meta)
	})
}
