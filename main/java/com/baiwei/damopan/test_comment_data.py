# !/usr/bin/python
# coding: utf-8

"""
@Project 
@File    test_comment_data.py
@Author  zhouhan
@Date    2025/11/12 12:25
"""

import os
import time
import random
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import ujson
import SiliconFlowApi

# 初始化日志配置
logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	handlers=[
		logging.FileHandler('spider_amap_weather.log'),
		logging.StreamHandler()
	]
)
logger = logging.getLogger(__name__)

# 读取环境变量
load_dotenv()
mssql_ip = os.getenv("sqlserver_ip")
mssql_port = os.getenv("sqlserver_port")
mssql_user_name = os.getenv("sqlserver_user_name")
mssql_user_pwd = os.getenv("sqlserver_user_pwd")
mssql_order_db = os.getenv("sqlserver_db")
mssql_db_schema = os.getenv("sqlserver_db_schema")

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 50)
pd.set_option('display.width', None)


def get_sensitive_words_list():
	"""读取敏感词文件，返回列表"""
	res = []
	try:
		with open("../resource/data/sensitiveword/Identify-sensitive-words.txt", "r", encoding="utf-8") as f:
			for line in f:
				res.append(line.strip())
	except IOError as e:
		print(e)
	return res


def get_random_element(lst):
	"""从列表中随机返回一个元素"""
	if not lst:
		return None
	return random.choice(lst)


# 预读取敏感词列表
words_list = get_sensitive_words_list()


def generate_user_comment_data():
	logger.info("开始生成用户评论数据...")

	# noinspection SqlResolve
	query_mssql_order_sql = """
        select top (abs(checksum(newid())) % 5 + 5)
               order_id,
               user_id,
               product_id,
               product_name,
               ds,
               ts
        from realtime_v3.dbo.oms_order_dtl
        order by newid();
    """

	# noinspection SqlResolve
	query_mssql_comment_sql = """
        select id,
               order_id,
               user_id
        from realtime_v3.dbo.oms_order_user_comment;
    """

	# noinspection SqlResolve
	insert_sql = text("""
        INSERT INTO realtime_v3.dbo.oms_order_user_comment
        (order_id, user_id, user_comment, ds, ts)
        VALUES (:order_id, :user_id, :user_comment, :ds, :ts)
    """)


	insert_buffer = []

	mssql_connection = (
		f"mssql+pymssql://{mssql_user_name}:{mssql_user_pwd}"
		f"@{mssql_ip}:{mssql_port}/{mssql_order_db}"
	)
	mssql_engine = create_engine(mssql_connection)

	logger.info("正在加载订单数据...")
	mssql_origin_order_df = pd.read_sql_query(query_mssql_order_sql, mssql_engine)
	mssql_origin_comment_df = pd.read_sql_query(query_mssql_comment_sql, mssql_engine)

	filtered_df = mssql_origin_order_df[
		(~mssql_origin_order_df['order_id'].isin(mssql_origin_comment_df['order_id'])) &
		(~mssql_origin_order_df['user_id'].isin(mssql_origin_comment_df['user_id']))
		]

	records_list = filtered_df.to_dict(orient='records')
	logger.info(f"获取到 {len(records_list)} 条待生成评论的数据。")

	for i in records_list:
		try:
			if random.random() < 0.4:
				comment_type = "bad"
				build_comment = f'给出这个商品的评论,差评, 攻击性拉满,使用不文明用语,《${i["product_name"]}》'
			else:
				comment_type = "good"
				build_comment = f'给出这个商品的评论,好评, 语气真诚,表达赞美和满意,《${i["product_name"]}》'

			logger.info(f"正在生成评论 (order_id={i['order_id']}, type={comment_type})")

			resp = SiliconFlowApi.call_silicon_model(
				'Pro/deepseek-ai/DeepSeek-R1-Distill-Qwen-7B',
				build_comment
			)

			try:
				data = ujson.loads(resp)
				i['user_comment'] = data.get('user_comment', str(resp))
			except Exception:
				logger.warning(f"JSON解析失败，原始响应: {resp}")
				i['user_comment'] = str(resp)

			# 30% 概率追加敏感词
			if random.random() < 0.3:
				bad_word = get_random_element(words_list)
				if bad_word:
					if random.random() < 0.5:
			 			i['user_comment'] = f"{bad_word} {i['user_comment']}"
					else:
						i['user_comment'] = f"{i['user_comment']} {bad_word}"

			# 删除无用字段
			i.pop('product_id', None)
			i.pop('product_name', None)

			# 添加到批量写入 buffer
			insert_buffer.append(i)

			# 每 10 条写入一次
			if len(insert_buffer) >= 10:
				with mssql_engine.begin() as conn:
					conn.execute(insert_sql, insert_buffer)
				logger.info(f"成功写入 {len(insert_buffer)} 条评论到 SQL Server")
				insert_buffer = []

			logger.info(f"生成成功: order_id={i['order_id']} user_id={i['user_id']} type={comment_type}")
			print(i)

			time.sleep(2)

		except Exception as e:
			logger.error(
				exc_info=True
			)
			continue

	# 处理不足 10 条的剩余数据
	if insert_buffer:
		logger.info(f"成功写入剩余 {len(insert_buffer)} 条评论到 SQL Server")

	logger.info("全部评论生成完成。")


if __name__ == '__main__':
	generate_user_comment_data()
