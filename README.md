# google maps place data collect 谷歌地图PLACE数据采集

### 首先生成.env文件
```angular2html
cp .env.example .env
```
### 修改.env文件中的配置

> 填好google cloud 的 api key
> 
> 确认存储目录，默认是data
> 
> 修改采集参数，默认采集新加坡数据（程序中专门为新加坡数据做了过滤，如果采集其他地区数据，程序需修改过滤）

### 执行采集
```base
pip install -r requirements.txt
python main.py
```

### 默认数据放在data目录下，可以通过.env文件修改