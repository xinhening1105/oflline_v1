#!/usr/bin/python
# coding: utf-8

"""
@Project
@File    SiliconFlowApi.py
@Author  zhouhan
@Date    2025/11/12 15:51
"""
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

def call_silicon_model_direct(p_model_name, p_prompt):
	"""直接使用requests调用API，避免openai库的编码问题"""
	silicon_api_token = os.getenv("siliconflow_token")

	if not silicon_api_token:
		raise ValueError("siliconflow_token not set")

	url = "https://api.siliconflow.cn/v1/chat/completions"

	headers = {
		"Authorization": f"Bearer {silicon_api_token}",
		"Content-Type": "application/json"
	}

	data = {
		"model": p_model_name,
		"messages": [
			{
				"role": "system",
				"content": "You are a helpful assistant that outputs JSON."
			},
			{
				"role": "user",
				"content": f"{p_prompt}\n\nRespond in JSON: {{\"user_comment\": \"text\"}}"
			}
		],
		"temperature": 0.7,
		"max_tokens": 512,
		"stream": False,
		"response_format": {"type": "json_object"}
	}

	try:
		response = requests.post(url, headers=headers, json=data, timeout=30)
		response.raise_for_status()
		result = response.json()
		return result["choices"][0]["message"]["content"]
	except Exception as e:
		return json.dumps({"user_comment": f"Request failed: {str(e)}"})

def test_direct_api():
	"""测试直接API调用"""
	print("Testing direct API call...")

	# 纯英文测试
	prompt = "Write a simple product review"
	result = call_silicon_model_direct('Pro/deepseek-ai/DeepSeek-R1-Distill-Qwen-7B', prompt)

	print("Direct API call completed")
	print(f"Response: {result}")

	try:
		data = json.loads(result)
		print(f"Comment: {data.get('user_comment', 'No comment field')}")
	except:
		print("Could not parse JSON")

if __name__ == '__main__':
	test_direct_api()