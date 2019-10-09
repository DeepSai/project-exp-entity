# project-exp-entity
项目经历实体，也是项目经历摘要，从cv的项目经历中抽取若干短标签，作为对项目的概括描述。

## step 1
从spark平台上聚合相关的cv字段：包括 基本信息中的project、work，算法解析信息中的cv_tag。
见`cv_pro_alg.py`, `cv_pro_alg.sh`

## step 2
从join后的json数据中抽取所需内容：重点是将项目经历对应于工作经历，进而对应于算法识别出function，然后提取该段项目经历的名称，描述，职责，职能。
见`cluster_by_func.py`

## step 3
实体化，即抽取标签：
1. 名称 
2. skill
3. 职能

