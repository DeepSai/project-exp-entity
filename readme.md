# project-exp-entity
项目经历实体，也是项目经历摘要，从cv的项目经历中抽取若干短标签，作为对项目的概括描述。

## step 1
将数据从spark平台上拉到本地处理 速度太慢，所以先在spark平台上抽取所需要的字段，再拉到本地进行json解析。

从spark平台上抽取相关的cv字段：包括 基本信息中的project、work，算法解析信息中的cv_tag。
见`cv_pro_alg.py`, `cv_pro_alg.sh`

## step 2
从join后的json数据中抽取所需内容：提取项目经历名称，项目描述，项目职责，四级、三级、二级职能。难点在于将项目经历对应到工作经历，进而对应到算法识别出职能，见 `cluster_by_func.py`

## step 3
实体化，抽取标签：
1. 项目名称 
2. 业务标签
3. skill
4. 职能

