import re
import os
import sys
import copy
import json
import time
import logging
import requests
import threading
from tqdm import tqdm
from zhipuai import ZhipuAI
from configs.prompt_config import *
from configs.sql_relation_config import *
from concurrent.futures import ThreadPoolExecutor, TimeoutError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from configs.api import api_key, sql_api_key, api_key_backup, api_key_backup_2


specific_logger = logging.getLogger('specific_logger')
specific_logger.setLevel(logging.INFO)
try:
    specific_logger.addHandler(logging.FileHandler("./devlop_home/logs/app.log"))
    print('当前环境为docker')
except Exception as e:
    print('当前环境为非docker')
    specific_logger.addHandler(logging.FileHandler("./logs/app.log"))
specific_logger.propagate = False


class all_agent_node:
    def __init__(self):
        self.url_sql = "https://comm.chatglm.cn/finglm2/api/query"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {sql_api_key}"
        }
        self.data_index_annotation = data_index_annotation
        self.prompt_recursion_search_node = prompt_recursion_search_node_raw
        self.prompt_check_answer_node_child = prompt_check_answer_node_child_raw
        self.prompt_build_relation_node = prompt_build_relation_node_raw
        self.prompt_all_example = prompt_all_example_raw
        self.prompt_check_answer_node = prompt_check_answer_node_raw
        self.client = ZhipuAI(api_key=api_key)
        self.response_key = ['reason_content', 'sql_command', 'answer']
        self.key_error_num = 0
        self.key_name_container = {}
        self.void_num = 0
        self.backup_api = 0
        self.libraries_infosource = {
            "astockbasicinfodb.lc_business":"年度报告,半年报,2019年半年度报告,2019年度报告",
            "astockbasicinfodb.lc_namechange":0,
            "astockeventsdb.lc_credit":0,
            "astockeventsdb.lc_entrustinv":0,
            "astockeventsdb.lc_majorcontract":0,
            "astockeventsdb.lc_regroup":0,
            "astockeventsdb.lc_suitarbitration":0,
            "astockeventsdb.lc_warrant":0,
            "astockfinancedb.lc_auditopinion":"半年度报告,年度报告",
            "astockfinancedb.lc_balancesheetall":"半年度报告,年度报告",
            "astockfinancedb.lc_capitalinvest":0,
            "astockfinancedb.lc_cashflowstatementall":"半年度报告,年度报告",
            "astockfinancedb.lc_incomestatementall":"半年度报告,年度报告",
            "astockfinancedb.lc_mainoperincome":"半年度报告,年度报告,年度报告(更正后)",
            "astockfinancedb.lc_operatingstatus":"2019第一季度报告,2019第三季度报告,2019年度报告,2020第一季度报告,2020第三季度报告,2020年度报告,2021第一季度报告,2021第三季度报告,2021第一季度报告正文,2020年度报告摘要,2020第一季度报告正文,2021年度报告,2021年度报告摘要",
            "astockindustrydb.lc_exgindchange":0,
            "astockindustrydb.lc_exgindustry":0,
            "astockmarketquotesdb.lc_suspendresumption":"信息来源是代码，分别有90和83，对应深交所和上交所。",
            "astockoperationsdb.lc_rewardstat":"年度报告",
            "astockoperationsdb.lc_staff":"半年报,年度报告",
            "astockoperationsdb.lc_suppcustdetail":"半年度报告,定期报告:年度报告,临时公告:年度报告(更正后),年度报告,定期报告:半年度报告",
            "astockshareholderdb.lc_buyback":0,
            "astockshareholderdb.lc_legaldistribution":0,
            "astockshareholderdb.lc_mainshlistnew":"半年报,年度报告",
            "astockshareholderdb.lc_mshareholder":"年度报告,半年报,2018年年报",
            "astockshareholderdb.lc_sharefp":0,
            "astockshareholderdb.lc_sharefpsta":0,
            "astockshareholderdb.lc_sharestru":"年度报告,半年报,2021年第三季度报告",
            "astockshareholderdb.lc_sharetransfer":0,
            "astockshareholderdb.lc_shnumber":"年度报告,半年报,2021年第三季度报告的更正公告,半年度报告",
            "astockshareholderdb.lc_stockholdingst":"年度报告,半年报",
            "astockshareholderdb.lc_transferplan":0,
        }

    def sql_execute_node(self, data):
        """sql 查询节点"""
        while True:
            try:
                response_sql_execute_node = requests.post(self.url_sql, headers=self.headers, json=data, timeout=60)
        
                if response_sql_execute_node.status_code == 200:
                    dict_data = response_sql_execute_node.json()['data']
                    dict_data_count = response_sql_execute_node.json()['count']
                    
                    if type(dict_data) == list and len(dict_data) == 0:
                        self.void_num += 1
                        if 'HighPrice' in data['sql'] or 'LowPrice' in data['sql']:
                            return ['0', '当前查询结果为空。查询内容中存在最高价或最低价内容，可能是某类型最高价或最低价对应的键没有内容，所以可以查询题设范围内的每日最高价或最低价，然后按照要求进行MAX或MIN，这样就能求出对应的最高价或最低价。', None]
                        elif self.void_num == 2:
                            self.void_num = 0
                            return ['0', '当前查询结果为空。如果同目的查询已多次查询结果为空，可能是因为没有情况满足题设条件。例如查询某公司是否更名过时，若多次查询为空，则表明公司应该是没有更名过。', None]
                        return ['0', '当前查询结果为空，请查询该表所有内容或查询其他键以确认具体查询条件。', None]
                    else:
                        # 截取掉结果中超出长度的内容
                        for key in dict_data[0]:
                            if len(str(dict_data[0][key])) > 1000:
                                dict_data[0][key] = None
                                specific_logger.info('部分数据过长，已被截取')
                        if dict_data_count > 50:
                            return ['0', '查询输出数量过多，请重新生成对应的SQL语句。如果是统计任务，请使用COUNT进行查询。如果是查询某种特定代码，请务必使用DISTINT来重新查询，否则有可能查询出来超多同样的结果，污染上下文。', dict_data_count]
                        else:
                            return ['1', dict_data, dict_data_count] # 如果查询成功，则返回成功数据, 形式是列表中包含字典元素
                elif response_sql_execute_node.status_code == 500:
                    Erro_message = response_sql_execute_node.json()['detail']
                    if "No database selected" in str(Erro_message):
                        return ['0', '查询语句中数据库撰写错误，请确保所选择的数据库是在系统提示词中明确说明的数据库。否则会继续报错，请认真检查并修改过来！！！', None]
                    return ['0', Erro_message, None] # 如果查询失败，则返回查询失败详细报告
                elif response_sql_execute_node.status_code == 401:
                    specific_logger.info('sql_api_key 错误' + json.dumps(response_sql_execute_node))  
                    time.sleep(5)
                    return ['0', 'sql_api_key 错误', None]
            except requests.Timeout:
                try:
                    data_try = { 
                        "sql": "SELECT * FROM usstockdb.us_companyinfo LIMIT 1",
                        'limit': 1
                    }
                    response_sql_execute_node = requests.post(self.url_sql, headers=self.headers, json=data_try, timeout=20)
                except requests.Timeout:
                    specific_logger.info("数据库查询超时，当前是SQL服务器存在问题。即将休眠5分钟后继续查询。")
                    time.sleep(300)
                    continue
                specific_logger.info('数据库超时，原因是查询语句有问题，数据库无法处理。')
                return ['0', '数据库超时，原因是查询语句有问题，数据库无法处理。', None]
            except Exception as e:
                specific_logger.info(f"数据库查询出现错误，错误原因为: {str(e)}")
                print(f"数据库查询出现错误，错误原因为: {str(e)}")
                time.sleep(10)
                continue
    
    def search_all_sample(self, answer_new_list):
        while True:
            results = []
            inquire_comand_list = []
            all_examples = ''
            
            for library_name in answer_new_list:
                inquire_comand = f"SELECT * FROM {library_name} LIMIT 1"
                
                inquire_comand_list.append(inquire_comand)

                data = {
                    "sql": inquire_comand,
                    "limit": 1
                }
                
                result = self.sql_execute_node(data) # 此处的返回值的形式是列表，第一个元素是‘0’和‘1’，第二个元素是字典
                
                if result is None:
                    specific_logger.info('数据库查询无响应')
                    continue
                
                example_head = f'执行{inquire_comand}的sql查询结果如下:\n'

                key_now = ''
                for key in result[1][0]:
                    try:
                        result[1][0][key] = '该键解释为：' + str(data_index_annotation[key]) + ' 为了防止你乱使用样例数据，样例数据被隐藏。'
                        key_now += key + ','
                    except KeyError:
                        pass
                
                all_examples += f'{library_name}这个库表的作用如下：\n' + Astock_data_relation_only[library_name] + '\n' + f'**{library_name}的数据库键的含义如下:**：\n' + json.dumps(result[1][0], indent=2, ensure_ascii=False) + '\n' + f'查询{library_name}库时只能查询这些键:{key_now}，请勿将其他键用在此库中查询。' + '\n\n'

                results.append(result)
            
            if any(result[0] == '0' for result in results):
                specific_logger.info('数据库连接错误，重新查询')
                continue
            else:
                break
        return all_examples, results
    
    def recursion_search_node(self, all_example, query_list, search_unicode_answer, library_name_list, search_path):
        
        num_kill = 0 # 陷入死循环时终止使用
        
        num_stop = 0 # 终止循环的次数
        
        query_head = '**用户当前问题**：\n'
        
        answer_list = []
        
        query_num = 1
        
        SQL_container = {}
        
        history_query_answer = ''
        
        Astock_database_container_str = ""
        
        for key in Astock_database_container:
            Astock_database_container_str += key + '\n'
        
        prompt_recursion_search_node = self.prompt_recursion_search_node.format(Astock_database=Astock_database_container_str)
                
        prompt_all_example = self.prompt_all_example.format(sample=all_example, query='问题1：' + query_list[0], search_unicode_answer=search_unicode_answer)
                
        messages = [
            {"role": "system", "content": prompt_recursion_search_node},
            {"role": "user", "content": prompt_all_example + '\n' + '请根据以下计划进行查询：\n' + search_path + '\n' + '若该查询路径无结果，则请你寻找其他可以查询路径。'}
        ]
        
        tools_head = '工具调用完毕，以下是工具的调用结果：\n'
        
        flag_break = 0
        flag_break_trading = 0
        flag_check_search_break = 0
        flag_break_yuan = 0
        tip_infosource_flag = 1
        flag_tradingday = 0
        flag_R = 1
        answer_container = {}

        while True: 
            specific_logger.info('递归查询节点正在运行...')
            SQL_resopnse_content = '以下是执行SQL命令的结果：\n'
            
            num_stop += 1
            
            if num_stop == 25:
                answer_list.append('')
                return answer_list
                
            answer_json_raw = self.model_call(messages, temperature=0.3)
            
            if 'reason_content' not in answer_json_raw or 'answer' not in answer_json_raw or 'sql_command' not in answer_json_raw:
                specific_logger.info('模型输出内容不符合要求，重新生成')
                messages.append({"role": "assistant", "content": answer_json_raw})
                messages.append({"role": "user", "content": '你上述输出的内容不满足系统提示次要求的JSON格式的数据，请重新按照系统提示中要求的输出格式进行输出。'})
                continue
            
            specific_logger.info('模型回答已生成，内容如下：')
            specific_logger.info(answer_json_raw)
                
            answer_json = self.load_json(answer_json_raw)

            if answer_json is None:
                messages.append({"role": "assistant", "content": answer_json_raw})
                messages.append({"role": "user", "content": '你上述输出的内容不满足JSON格式的数据，请重新按照系统提示中要求的输出格式进行输出，输出内容必须只能是JSON格式的数据。请勿漏了```json```以及请勿漏了转义字符。'})
                specific_logger.info('输出的json格式严重错误，重新生成')
                continue
            
            if '假设' in answer_json['reason_content']:
                messages.append({"role": "assistant", "content": answer_json_raw})
                messages.append({"role": "user", "content": "你的推理内容中出现'假设'二字，请检查是否出现查询结果多于目标查询数量，如果是，请你重新确认查询范围。一般这种错误出现在时间约束错误，请你重新查看题目中的时间要求，如果没有明确说是哪一年，那这个代指应该是说具体的时间，或该时间之前。请你检查好问题所在后再按照原来的要求进行输出。请勿再次出现假设二字"})
                specific_logger.info('出现假设内容，重新生成')
                continue

            if any(k not in answer_json for k in self.response_key):
                specific_logger.info(f'第{num_stop}交互时，输出格式不符合要求')
                messages.append({"role": "assistant", "content": answer_json_raw})
                messages.append({"role": "user", "content": '你的输出不符合键的要求，请确保你的输出中含有系统提示词中规定的三种键。'})
                continue # 输出格式存在问题
            else:
                specific_logger.info(f'第{num_stop}交互时，输出格式符合要求')
                    
            if answer_json['sql_command'] == '' and query_num < len(query_list):
                
                # 修改经典的数据格式错误
                
                answer_json['answer'] = self.check_num(answer_json['answer'])
                
                if answer_json['answer'] in answer_container:
                    specific_logger.info('出现重复回答，重新生成')
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '你的回答与之前的回答重复，请重新回答。你应该是没有查询看当前需要解决的问题，请你集中注意力解决当前问题，具体问题请你查看上文中的记录。'})
                    continue
                
                if flag_break == 0:
                    specific_logger.info('正在检查是否存在口算内容。')
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '请检查你的回答数据是否来自于SQL计算或具体查询，如果是口算的结果，请使用SQL语句进行计算。如果回答的数据均来自于查询或SQL语句计算，若计算百分比时没有在SQL语句中乘100，则重新计算。若已按要求计算，请重新输出原回答内容。再次特别强调：如果题目求解的是带有%号的，计算时记得乘100。也就是所有类似百分比，比例这样的，一定要在计算的时候就要乘100，然后才进行有效位数保留。你很容易犯的错误是，你在SQL语句中已经对计算的内容乘了100，然后你在回答中又挪了两个小数点，请你认真检查，如果你在SQL语句中明确乘了100，你就不要再挪小数点了。（特别注意：分红派现比例请勿使用%符号，按原始查询进行输出）'})
                    flag_break += 1
                    continue
                
                if '参考查询内容' in answer_json['answer'] and flag_break == 0:
                    specific_logger.info('正在检查是否存在口算内容。')
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": "你的输出不完整，请勿输出类似'参考查询内容'这样的内容，请按照要求回答完整。"})
                    continue
                
                if ('亿' in answer_json['answer'] or ('万元' in answer_json['answer'] and '万元' not in query_list[query_num - 1])) and flag_break_yuan == 0:
                    specific_logger.info('单位可能有错误。')
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": "请检查题目是否有明确约束单位。如果题目没有明确说明需要使用万元或亿这样的单位，请你对原始查询数据进行有效位数省略，然后将回答部分修正过来。若原始数据有效位数小于要求位数，则补充到题目要求的有效位数。如果题设要求万元或亿元为单位，则不需要修改你所回答的内容。例如题目询问'天士力在2020年的最大担保金额是多少？答案需要包含1位小数'，题中并没有要求以万元或亿为单位，则使用原始查询数据进行有效位数保留即可，如果原始查询数据是1620000000.0（举例子，具体数据请参考实际查询结果），则直接输出1620000000.0（举例子，具体数据请参考实际查询结果）。"})
                    flag_break_yuan += 1
                    continue
                
                if flag_check_search_break == 0 and self.check_search_data(answer_json['answer'], query_list[query_num - 1]) == '0':
                    specific_logger.info('回答中存在摆烂。')
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": "你现在有可能正在犯不查询数据库就回答的错误或把问题的某个字问题给遗忘了或者回答主体不完整。\n例如你的回答中含有'由于没有XXX的XX数据'这样的字样或者说你没有回答第子问题，但是你未使用SQL语句查询过其他基金公司的同期数据，因此请你务必先按照系统提示词输出相应格式的SQL查询语句来查询相关数据。如果你是因为查询了很多次还是查询不到，则可以重新输出原来的内容。\n还有一种情况就是你算出来的比例大于100%，一般是由于你没有约束时间或者没有约束信息来源，请你检查是哪一种。\n还有一种摆烂情况是题目询问的是多个时间点的具体情况，而你由于看到查询结果一样，将它们融合在了一块，但实际上要分别说明具体情况，例如回答某三年内的人员情况，就需要把这三年的三个情况都列完整出来。\n还有一种情况就是，通过排序查询，有两个公司重复，但题目要查询的是三个公司，说明某个指标中某个公司满足该排序多次，那这个时候就需要查询出满足条件的三个不同的公司。\n如果在题目没有明确说明回答需要代码时，请回答具体的中文名称，例如询问哪支基金的规模最大，则必须回答出对应的基金名称。"})
                    flag_check_search_break += 1
                    continue
                if any('XX' in s for s in answer_json['answer']):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '你的回答中有XX这样的内容，说明你回答摆烂了。请你继续按照系统提示词要求输出对应的内容来进行查询相关内容，回答需要是具体内容。'})
                    specific_logger.info('回答中有XX关键字')
                
                answer_list.append(answer_json['answer'])
                
                self.logging_color('当前问题的回答为\n' + answer_json['answer'] + '\n\n', "34")
                history_query_answer += f'问题{query_num}:{query_list[query_num - 1]}\n{answer_json["answer"]}\n'
                search_plan = self.plan_node(all_example, f'前置信息为{search_unicode_answer}\n' + history_query_answer + '\n\n当前问题为：' +query_list[query_num])
                specific_logger.info('已解决当前问题，即将进入下一题解决')
                answer_container[answer_json['answer']] = 1
                flag_break = 0
                flag_break_trading = 0
                flag_check_search_break = 0
                flag_break_yuan = 0
                tip_infosource_flag = 1
                flag_tradingday = 0
                num_kill = 0
                flag_R = 0
                messages.append({"role": "assistant", "content": answer_json_raw})
                messages.append({"role": "user", "content": f'历史问答为：\n{history_query_answer}\n请回答继续根据系统提示词要求查询数据库并回答以下问题：\n当前你要处理的的问题{query_num + 1}为:' + query_list[query_num] + '\n' + '请根据以下计划进行查询：\n' + search_path + '\n' + '若该查询路径无结果，则请你寻找其他可以查询路径。'})
                query_num += 1
                continue
                
            elif answer_json['sql_command'] == '' and query_num == len(query_list) and answer_json['answer'] != '':
                answer_json['answer'] = self.check_num(answer_json['answer'])
                if answer_json['answer'] in answer_container:
                    specific_logger.info('出现重复回答，重新生成')
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '你的回答与之前的回答重复，请重新回答。你应该是没有查询看当前需要解决的问题，请你集中注意力解决当前问题，具体问题请你查看上文中的记录。'})
                    continue
                if flag_break == 0:
                    specific_logger.info('正在检查是否存在口算内容。')
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '请检查你的回答数据是否来自于SQL计算或具体查询，如果是口算的结果，请使用SQL语句进行计算。如果回答的数据均来自于查询或SQL语句计算，则重新输出原回答内容。也就是所有类似百分比，比例这样的，要在计算的时候就要乘100，然后才进行有效位数保留。例如查询利亚德光电在2019到2021年这三年间技术人员和生产人员占职工总数的比例分别是多少？这样的问题，一定要使用SQL的计算语句进行计算，请务必在sql_command中实现，不要口算！！！'})
                    flag_break += 1
                    continue
                if flag_check_search_break == 0 and self.check_search_data(answer_json['answer'], query_list[query_num - 1]) == '0':
                    specific_logger.info('回答存在摆烂。')
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": "你现在有可能正在犯不查询数据库就回答的错误或把问题的某个字问题给遗忘了。例如你的回答中含有'由于没有其他基金公司的同期数据'这样的字样或者说你没有回答第子问题，但是你未使用SQL语句查询过其他基金公司的同期数据，因此请你务必先按照系统提示词输出相应格式的SQL查询语句来查询相关数据。如果你是因为查询了很多次还是查询不到，则可以重新输出原来的内容。如果你觉得你没摆烂，请按照原来的内容输出！！！"})
                    flag_check_search_break += 1
                    continue
                if ('亿' in answer_json['answer'] or ('万元' in answer_json['answer'] and '万元' not in query_list[query_num - 1])) and flag_break_yuan == 0:
                    specific_logger.info('单位可能有错误。')
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": "请检查题目是否有明确约束单位，如果没有明确说明需要使用万元或亿这样的单位，请你对原始查询数据进行有效位数省略，然后将回答部分修正过来，请勿出现亿或万元的单位。若原始数据有效位数小于要求位数，则补充到题目要求的有效位数。"})
                    flag_break_yuan += 1
                    continue
                if any('XX' in s for s in answer_json['answer']):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '你的回答中有XX这样的内容，说明你回答摆烂了。请你继续按照系统提示词要求输出对应的内容来进行查询相关内容，回答需要是具体内容。'})
                    specific_logger.info('回答中有XX关键字')
                    continue
                answer_list.append(answer_json['answer'])
                specific_logger.info('所有问题已回答完毕，结束搜索\n')
                
                for m in messages:
                    specific_logger.info(f'{m["role"]}: \n{m["content"]}\n\n')
                
                return answer_list
            elif answer_json['sql_command'] == '' and answer_json['answer'] == '':
                specific_logger.info('未回答当前问题')
                messages.append({"role": "assistant", "content": answer_json_raw})
                messages.append({"role": "user", "content": '你的answer部分未输出，请检查你是否无法得到答案还是你没有及时把内容输出到answer中。如果是无法解决，则在answer中输出无法解决当前问题。'})
                continue
               
            if answer_json['sql_command'] != '':
                            
                specific_logger.info(f'第{num_stop}交互时，输出的sql命令不为空, 执行此命令')
                
                if self.check_time(answer_json['sql_command']) == '0':
                    specific_logger.info('模型输出时间不规范，重新生成')
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '你的SQL查询代码中包含的时间格式不符合要求，时间格式要求为：YYYY-MM-DD HH:MM:SS.mmm。其中交易日的时间的HH:MM:SS部分均统一为12:00:00.000。请重新输出符合系统提示词要求的JSON格式内容。'})
                    continue
                            
                # 对命令进行分割，并去除空格
                try:
                    sql_command_list = [sql_command.strip() for sql_command in answer_json['sql_command'].split(';') if sql_command.strip()]
                except Exception as e:
                    specific_logger.info('sql命令分割错误')
                    continue       
                if any(('COUNT' in s and 'DISTINCT' not in s) for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '你的SQL查询代码中使用到COUNT关键字，但没有去重统计，这会导致统计错误，所以请你请原来的输出的基础上进行去重处理，请使用像InnerCode，CompanyCode，ID，ObjectName等能代表被统计对象的唯一编码进行去重。请你判断好使用什么键去重，如果统计的事股票的数量，则用对应的InnerCode或CompanyCode进行去重。如果统计的事某种时间发生的次数，则使用ID进行去重，请勿使用其他键进行去重。请勿使用主体名称或其他具体名称进行去重，否则有可能引发错误。请你的回答是基于你上一个回答的基础上，修改成去重统计。你的统计代码还没被执行，因此你还没有得到任何数据支持，所以请你不要回答问题，请你务必先修改原来输出错误的内容。'})
                    specific_logger.info('没有进行去重处理')
                    continue
                if any('`' in s for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '你的SQL查询代码中包含有特殊字符，这会导致查询错误，请删除特殊字符。'})
                    specific_logger.info('有特殊字符')
                    continue
                if any("SHKind = '机构'" in s for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": "请勿使用SHKind = '机构'查询股东是否为机构，应该使用‘SELECT SHKind FROM astockshareholderdb.lc_mainshlistnew WHERE 具体约束条件’查询出具体的SHKind。然后然后通过具体内容来判断是否是基金公司或投资公司来判断是否是机构，计算的时候不要去重统计，也就是说如果出现N个一样的机构类型，算N个，例如出现三个开放式投资基金和一个投资、咨询公司，就按照3+1=4个机构来统计，请勿去重！！！。注意投资公司和基金都算是机构。"})
                    specific_logger.info("出现了使用SHKind = '机构'查询股东是否为机构")
                    continue
                if any('INTEGER' in s for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": 'INTEGER和INT不是当前查询的数据库中的有效数据类型，正确的类型应该是SIGNED 或 UNSIGNED。'})
                    specific_logger.info('不是有效查询数据类型。')
                    continue
                if any('TOP' in s for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '你的SQL查询代码中包含有TOP这样的关键字， MySQL 数据库不支持这样的语法，请按要求重新输出对应的内容。'})
                    specific_logger.info('SQL语句错误')
                    continue
                if any('PCTOfTotalShares * 100' in s for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '从PCTOfTotalShares获得的值的范围就是0-100，不能在乘100，它的含义就是百分比，请将该错误修改过来并重新按要求输出。'})
                    specific_logger.info('发生PCTOfTotalShares * 100错误')
                    continue
                if any('AS ChangePCTRY' in s for s in sql_command_list) or any('MAX(ChangePCTRY)' in s for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '年涨幅只需要使用ChangePCTRY查询即可获得，但现需要使用MAX（TradingDay）获得该年最后一个交易日，然后再查询该交易日的ChangePCTRY，若要查询出某家公司最大的ChangePCTRY，请使用ORDER BY ChangePCTRY DESC LIMIT 1进行查询，请勿使用其他方式计算。请查询astockmarketquotesdb.qt_stockperformance'})
                    specific_logger.info('发生年涨幅查询错误')
                    continue
                if any("SuspendStatement LIKE '%重大事项%'" in s for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": 'SuspendStatement键查询值只能是对应的代码，例如重大事项的代码是103。'})
                    specific_logger.info('发生重大事项查询错误')
                    continue
                if any('XX' in s for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '你的SQL查询代码中包含有XX这样的关键字，请你将XX替换成具体的查询名称，并按照系统提示要求重新作出回答。'})
                    specific_logger.info('有XX关键字')
                    continue
                if any('申万' in s for s in sql_command_list) and any('COUNT' in s for s in sql_command_list) and any('constantdb.secumain' in s for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '如果你当前是在统计某申万行业中的公司数量，请勿对constantdb.secumain进行关联查询，因为constantdb.secumain有可能缺失某些申万行业公司信息。'})
                    specific_logger.info('犯查询错误')
                    continue
                if any(("astockindustrydb.lc_conceptlist" in s and "ConceptState" not in s) for s in sql_command_list):
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": '查询astockindustrydb.lc_conceptlist库时，请务必不要遗漏ConceptState = 1这个约束，否者会查询错误。请按照要求修改过来。'})
                    specific_logger.info('犯概念板块查询错误')
                    continue # 保证查询astockindustrydb.lc_conceptlist时，不会遗漏ConceptState
                if any(".TradingDay" in s for s in sql_command_list) and flag_tradingday == 0:
                    messages.append({"role": "assistant", "content": answer_json_raw})
                    messages.append({"role": "user", "content": "检查提醒：若查询中含有多个表，且需要TradingDay作为约束条件，你很容易在查询时出现漏TradingDay的错误。也就是说，例如要查询的有两个表，且该两个表中均有TradingDay键，同时题目也有对TradingDay进行约束，这时候你需要对两个表的交易时间都要约束清楚，例如：b.TradingDay = '具体题设要求时间' AND c.TradingDay = '具体题设要求时间'，或者b.TradingDay = c.TradingDay。如果这个你的输出并不需要该提醒，则重新输出原来的内容；如果该提醒有作用，则按照正确的条件约束来输出SQL语句。"})
                    specific_logger.info('检查时间约束是否有问题')
                    flag_tradingday = 1 # 每道题只检查一次
                    continue
                try:
                    if (any('RM' in s for s in sql_command_list) or any('RY' in s for s in sql_command_list) or any('RW' in s for s in sql_command_list)) and flag_R == 0 and any('BETWEEN' in s for s in sql_command_list):
                        messages.append({"role": "assistant", "content": answer_json_raw})
                        messages.append({"role": "user", "content": "检查提醒：当你查询近一周，近一个月，近一年等交指标时，该指标是指当天追溯到特定日期范围内，而你现在使用了BETWEEN，有可能你理解错误了该键的统计周期。如果你现在查询的是某个时间范围内某个交易指标的情况，请查询统计周期为天的指标，然后再进行排序等后处理。例如查询某公司的某年最高收盘价，请使用ClosePrice这个键进行排序处理，请勿使用HighestClosePriceRY和BETWEEN一起查询，因为统计周期不匹配。如果你没有犯这个错误，请重新输出你原来的内容。"})
                        specific_logger.info('检查RY,RW,RM')
                        flag_R = 1 # 每道题只检查一次
                        continue
                except Exception as e:
                    print('检查RY,RW,RM时发生错误')
                tip_infosource = ''
                tip_infosource = ''
                
                try:
                    for key in self.libraries_infosource:
                        if any(key in s for s in sql_command_list): # 在这些库中
                            if tip_infosource_flag == 1:
                                for sql_command in sql_command_list:
                                    if len(self.InfoSource_check(sql_command)) != 0 and self.InfoSource_check(sql_command) not in tip_infosource:
                                        tip_infosource +=  self.InfoSource_check(sql_command)
                                if len(tip_infosource) != 0:
                                    messages.append({"role": "assistant", "content": answer_json_raw})
                                    messages.append({"role": "user", "content": f"{tip_infosource}" + "首先检查用户询问中是否有查询经验说明需要使用InfoSource这个键，如果有，则请你检查你是否已按照查询经验要求约束了InfoSource这个键。若查询经验中无该要求，请忽略此提醒。\n如果你已约束，再次检查查询的值是否在上述说明的范围内，如果不是，请修改过来。\n若题目中规定了查询时间范围，请务必再次检查你的SQL语句中是否已约束了查询时间，请你务必不要遗漏时间约束，否则会导致查询错误。"})
                                    specific_logger.info('正在检查查询infosource是否有错误。')
                            tip_infosource_flag = 0 # 每道题只检查一次
                    if len(tip_infosource) != 0:
                        continue
                except Exception as e:
                    print('信息来源检查功能出错，错误是' + str(e))
                
                for sql_command in sql_command_list:
                    error_flag = 0 # 判断是否会犯经典错误
                    
                    if sql_command[-7:] == 'LIMIT 1' and len(sql_command.split(' ')) == 6:
                        data = {
                                "sql": sql_command,
                                "limit": 1
                        }
                    else:
                        data = {
                                "sql": sql_command,
                                "limit": 10000
                        }

                    # 跳过已执行过了的SQL命令
                    if sql_command not in SQL_container:
                        SQL_container[sql_command] = 1
                    else:
                        SQL_resopnse_content += f'{sql_command}已经在执行过了，如无必要请勿再次查询。\n' + '请使用其他键查询或减少查询键（有的键的值是空的）'
                        num_kill += 1
                        if num_kill == 3:
                            if query_num < len(query_list):
                                answer_list.append(answer_json['answer'])
                                specific_logger.info('当前问题陷入死循环，终止查询当前问题。')
                                messages.append({"role": "assistant", "content": answer_json_raw})
                                messages.append({"role": "user", "content": f'由于多次查询无果，停止回答上一题。\n请回答继续回答以下问题：\n问题{query_num + 1}' + query_list[query_num]})
                                query_num += 1
                                num_kill = 0
                            else:
                                answer_list.append(answer_json['answer'])
                                specific_logger.info('当前问题陷入死循环，终止查询。')
                                return answer_list
                        specific_logger.info('提示：此sql命令已经执行过')
                        
                    specific_logger.info('正在执行sql命令：' + sql_command)
                    sql_mutual_response = self.sql_execute_node(data)
                    
                    # 将已查询成功的样例库加入到容器中
                    if sql_command[-7:] == 'LIMIT 1':
                        sql_command_split = sql_command.split(' ')
                        if len(sql_command_split) == 6 and sql_command_split[3] not in library_name_list:
                            library_name_list.append(sql_command_split[3])
                            
                    extract_libraies = self.extract_libraies_node(sql_command)
                    
                    library_not_query = '当前未查询样例的库表有以下几个：\n'
                    
                    # 记录没有查询的样例库
                    if extract_libraies is not None:
                        for l in extract_libraies:
                            if l not in library_name_list:
                                self.logging_color('正在犯不查样例表就执行精准查询的错误', "31")
                                library_not_query += l + ','
                                error_flag = 1 # 如果是1，则表明有库未被查询
                    
                    # SQL服务器无响应            
                    if sql_mutual_response == None:
                        SQL_resopnse_content += f'**执行{sql_command}进行数据库查询失败，失败原因：查询无响应。请使用其他SQL语句查询。'
                        specific_logger.info('数据库查询失败，当前查询无响应')
                        continue    
                        
                    example_key_value_head = f'**当前执行{sql_command}的sql查询得到的数据库样例中的键的含义以及示例如下:**：\n'
                    
                    # 解释样例库（修改样例库中的内容）
                    if sql_command[-7:] == 'LIMIT 1' and len(sql_command.split(' ')) == 6:
                        for key in sql_mutual_response[1][0]:
                                try:
                                    sql_mutual_response[1][0][key] = '该键解释为：' + str(data_index_annotation[key])
                                except KeyError:
                                    pass

                    if sql_mutual_response[0] == '0': # 此处判断查询是否成功，如果查询失败，则返回查询失败信息，并拼接到SQL_resopnse_content中
                                    
                        specific_logger.info('数据库查询失败，需重新查询, 更新查询失败结果到交互内容中')
                        specific_logger.info('执行的查询为：' + str(data))     
                        ErrorMessage = sql_mutual_response[1]
                                    
                        specific_logger.info('查询失败信息:' + ErrorMessage + '\n')
                        
                        if error_flag == 0 and 'Unknown column' not in sql_mutual_response[1]:
                            SQL_resopnse_content += f'**执行{sql_command}进行数据库查询失败，请重新查询, 以下是查询失败原因**：\n' + ErrorMessage + '\n' + '请使用其他键进行查询（如果查询时间相关的内容，可以使用其他时间键进行约束，例如使用GuaranteeEndDate无法查询，则可以换成InfoPublDate,EndDate,BeginDate等来查询），或其他方式查询。有可能当前键在对应库表中无内容，请你尝试使用关联键进行关联查询，比如先查询到证券编码。' + '\n\n'
                        elif error_flag == 0 and 'Unknown column' in sql_mutual_response[1]:
                            SQL_resopnse_content += f'**执行{sql_command}进行数据库查询失败，请重新查询, 以下是查询失败原因**：\n' + str(sql_mutual_response[1]) + '\n当前查询键不在所查询的库里，请你重新确认当前查询库表中含有的键，根据当前查询库表中含有的键，并进行更换可查询的键进行下一步查询。\n\n'
                        else:
                            SQL_resopnse_content += f'**执行{sql_command}进行数据库查询失败，请重新查询, 以下是查询失败原因**：\n' + ErrorMessage + '\n' + library_not_query + '\n下一步请不要进行精确搜索，请你先查询上述未查询的库的样例，然后再进行精准的查询。\n\n'
                    else:
                        count_num = f'一共有{sql_mutual_response[2]}条查询结果满足{sql_command}的查询条件\n'
                        sql_response_head = f'**执行{sql_command}的sql查询结果如下:**：\n'
                        SQL_resopnse_content += sql_response_head + count_num + json.dumps(sql_mutual_response[1], indent=2, ensure_ascii=False) + '\n'

                    self.logging_color(SQL_resopnse_content + '\n\n', "32")
            
            messages.append({"role": "assistant", "content": answer_json_raw})
            messages.append({"role": "user", "content": tools_head + SQL_resopnse_content + '\n请你认真阅读上述工具调用结果。然后按照系统提示词要求，使用SQL查询工具，继续思考如何进行下一步的搜索或回答问题，请勿把信息获取动作扔回给用户，所有信息只能通过你使用SQL语句进行查询。'})
    
    
    def model_call(self, messages, model_name="glm-4-plus", temperature=0.2, max_tokens=4096, time_out=180):
        while True:
            def call_model():
                """ API 调用封装，返回 API 响应或异常 """
                try:
                    return self.client.chat.completions.create(
                        model=model_name,
                        messages=messages,
                        temperature=temperature,
                        max_tokens=max_tokens
                    )
                except Exception as e:
                    return e

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(call_model)
                try:
                    response = future.result(timeout=time_out)  # 等待 time_out 秒
                except TimeoutError:
                    print("请求超时，10秒后重试...")
                    time.sleep(10)  # 休息 10 秒后再试
                    continue  # 重新尝试

            if isinstance(response, Exception):
                print("模型调用失败，正在重试...失败原因是", str(response))
                if '欠费' in str(response) and self.backup_api == 0:
                    print('当前API已欠费，切换到备份API\n\n\n')
                    self.backup_api = 1
                    self.client = ZhipuAI(api_key=api_key_backup) 
                elif '欠费' in str(response) and self.backup_api == 1:
                    print('当前API已欠费，切换到备份API\n\n\n')
                    self.backup_api = 2
                    self.client = ZhipuAI(api_key=api_key_backup_2)  # 欠费后更换成备份API
                elif '欠费' in str(response) and self.backup_api == 2:
                    print('当前API已欠费，直接引发异常')
                    raise RuntimeError("终结程序")
                time.sleep(1)  # 避免短时间内高频请求
                continue  # 重新尝试

            try:
                return response.choices[0].message.content
            except Exception as e:
                print("解析 API 响应失败，正在重试...", str(e))
                time.sleep(1)  # 避免短时间高频请求
                continue  # 重新尝试
    
    def correct_json(self, json_content):
        """
        检查并纠正 JSON 格式
        """
        messages = [
            {"role": "system", "content": prompt_correct_json},
            {"role": "user", "content": json_content}
        ]
        corrected_json = self.model_call(messages, model_name='glm-4-plus')
        
        return corrected_json
    
    def load_json(self, json_content_raw):
        num_json_correct = 0
        while num_json_correct < 3:
            try:
                # 先把json内容抽取出来
                pattern = r'```json(.*?)```'
                match = re.search(pattern, json_content_raw, re.DOTALL)
                if match is not None:
                    json_content = match.group(1).strip()
                else:
                    json_content = json_content_raw
            except Exception as e:
                specific_logger.info('输出无json内容，请重新生成')
                return None
            try:
                json_content = json.loads(json_content)
                return json_content
            except json.JSONDecodeError as e:
                specific_logger.info(f"JSON 解析错误：{e}")
                specific_logger.info(f"正在尝试第{num_json_correct + 1}纠正 JSON 格式...")
                json_content_raw = self.correct_json(json_content_raw)
                num_json_correct += 1
                continue
        specific_logger.info('三次纠正均失败，请重新生成')
        return None
    
    def extract_libraies_node(self, sql_command):
        num_extract = 0
        while num_extract < 3:
            try:
                messages = [
                    {"role": "system", "content": prompt_whether_sql_command_is_execute},
                    {"role": "user", "content": f'请对下{sql_command}指令进行库表抽取，输出内容只有库表，请不要输出其他任何内容'}
                ]
                
                result = self.model_call(messages, model_name='glm-4-flash', temperature=0.1)

                result = [r.strip() for r in result.split(',')]
                
                if any(r not in sql_data_head for r in result):
                    num_extract += 1
                    continue
                return result
            except Exception as e:
                specific_logger.info('抽取发生错误')
                specific_logger.info(str(e))
        return None
    
    def logging_color(self, text, color_code):
        specific_logger.info(text)
    
    def search_unicode_node_pre(self, entity_list):
        tips = ''
        market = ''
        libraiy_key = {
            "astockindustrydb.lc_conceptlist": ["ClassName", "SubclassName", "ConceptName"],
            "constantdb.secumain": ["InnerCode", "CompanyCode", "SecuCode", "ChiName", "ChiNameAbbr", "EngName", "EngNameAbbr", "SecuAbbr", "ChiSpelling"],
            "constantdb.hk_secumain": ["InnerCode", "CompanyCode", "SecuCode", "ChiName", "ChiNameAbbr", "EngName", "EngNameAbbr", "SecuAbbr", "ChiSpelling"],
            "constantdb.us_secumain": ["InnerCode", "SecuCode", "SecuAbbr", "ChiSpelling", "EngName", "ChiName"],
            "usstockdb.us_companyinfo": ["CompanyCode", "EngName", "EngNameAbbr", "ChiName"],
            "astockindustrydb.lc_exgindustry": ["FirstIndustryName", "SecondIndustryName", "ThirdIndustryName"],
            "publicfunddb.mf_fundarchives": ["Manager"],
            "publicfunddb.mf_fundprodname": ["DisclName", "ChiSpelling"],
        }
        
        for entity in entity_list:
            for K in libraiy_key:
                for k in libraiy_key[K]:
                    data = { 
                            "sql": f"SELECT * FROM {K} WHERE {k} = '{entity}' LIMIT 1",
                            'limit': 1
                    }
                    response = self.sql_execute_node(data)
                    if response[0] == '1':
                        tips += f"\n在{K}库表中，令{k} = '{entity}'可以查询到相关内容。"
                        if K == "constantdb.secumain" and f'（{entity}是境内交易市场的证券相关主体（此处为泛化查询获得，若与前面题设冲突，以题设为准））' not in market:
                            market += f'（{entity}是境内交易市场的证券相关主体（此处为泛化查询获得，若与前面题设冲突，以题设为准））'
                        if K == "constantdb.hk_secumain" and f'({entity}是港股范畴的股票（此处为泛化查询获得，若与前面题设冲突，以题设为准）)' not in market and f'（{entity}是境内交易市场的证券相关主体（此处为泛化查询获得，若与前面题设冲突，以题设为准））' not in market:
                            market += f'({entity}是港股范畴的股票（此处为泛化查询获得，若与前面题设冲突，以题设为准）)'
                        if (K == "constantdb.us_secumain" or K == "usstockdb.us_companyinfo") and f'({entity}是美股范畴的股票（此处为泛化查询获得，若与前面题设冲突，以题设为准）)' not in market and f'({entity}是港股范畴的股票（此处为泛化查询获得，若与前面题设冲突，以题设为准）)' not in market and f'（{entity}是境内交易市场的证券相关主体（此处为泛化查询获得，若与前面题设冲突，以题设为准））' not in market:
                            market += f'({entity}是美股范畴的股票（此处为泛化查询获得，若与前面题设冲突，以题设为准）)'
        return [tips, market]
        
    def search_unicode_node(self, entity, tips):
        if len(tips) != 0:
            messages = [
                {"role": "system", "content": prompt_search_unicode},
                {"role": "user", "content": f'请你按照系统提示词要求的完成以下实体的相关内容的查询：\n'+ entity + f'\n请你先按照系统提示词，查询相关的库表的样例库\n以下是查询提示：\n{tips}'}
            ]
        else:
            messages = [
                {"role": "system", "content": prompt_search_unicode},
                {"role": "user", "content": f'请你按照系统提示词要求的完成以下实体的相关内容的查询：\n'+ entity + '\n请你先按照系统提示词，查询相关的库表的样例库'}
            ]
        num_kill = 0
        num_kill_force = 0
        while True:
            num_kill_force += 1
            if num_kill_force == 10:
                return '当前实体查询相关内容失败'
            result_raw = self.model_call(messages, model_name='glm-4-plus', temperature=0.3)
            
            try:
                pattern = r'```json(.*?)```'
                match = re.search(pattern, result_raw, re.DOTALL)
                if match is not None:
                    result = match.group(1).strip()
                else:
                    result = result_raw
            except Exception as e:
                specific_logger.info('json匹配有问题，问题如下' + str(e))
                
            try:
                result = json.loads(result)
            
                if any(key not in ['sql_command', 'reason_content', 'answer'] for key in result):
                    specific_logger.info('当前输出键不符合格式要求')
                    messages.append({"role": "assistant", "content": result_raw})
                    messages.append({"role": "user", "content": '你的输出不满足JSON的键要求，请重新生成，注意只能含有sql_command和reason_content两个键'})
                    continue
                
                elif "FirstIndustryCode" in result['sql_command'] and "SecondIndustryCode" in result['sql_command'] and "ThirdIndustryCode" in result['sql_command']:
                    specific_logger.info('犯了查询板块的经典错误')
                    messages.append({"role": "assistant", "content": result_raw})
                    messages.append({"role": "user", "content": '请勿同时查询不同级别板块代码，请重新按照系统提示词要求输出。'})
                    continue
                
                elif result['sql_command'] == '':
                    specific_logger.info('当前已完成搜索')
                    if entity.split(',')[0].strip() not in result['answer']:
                        specific_logger.info(entity.split(',')[0])
                        specific_logger.info(result['answer'])
                        messages.append({"role": "assistant", "content": result_raw})
                        messages.append({"role": "user", "content": f'你的输出没有指明原来的{entity}与回答内容的关系，请按照系统提示词要求重新输出，你的输出形式应该是：（查询的实体）的公司代码是：（具体公司代码）或（查询的实体）的概念代码是：（具体的概念代码）'})
                        specific_logger.info('当前输出内容与原来的实体没有直接显示关系')
                        continue
                    else:
                        return result['answer']
                else:
                    if result['sql_command'][-7:] == 'LIMIT 1':
                        data = {
                                "sql": result['sql_command'],
                                "limit": 1 
                        }
                        specific_logger.info('正在执行sql命令：' + result['sql_command'])
                        SQL_exec_result = self.sql_execute_node(data)
                        
                        if SQL_exec_result[0] == '0':
                            specific_logger.info('查询失败')
                            messages.append({"role": "assistant", "content": result_raw})
                            messages.append({"role": "user", "content": f"执行{result['sql_command']}查询失败，失败原因如下：\n" + json.dumps(SQL_exec_result[1], indent=2, ensure_ascii=False) + "\n请你使用其他方式查询。"})
                            num_kill += 1
                            if num_kill == 5:
                                messages = [
                                    {"role": "system", "content": prompt_search_unicode},
                                    {"role": "user", "content": '请你按照系统提示词要求的完成以下实体的相关内容的查询：\n'+ entity + '\n请你先按照系统提示词，查询相关的库表的样例库'}  
                                ]
                            continue
                        else:
                            specific_logger.info('样例查询成功，正在解释键含义')
                            for key in SQL_exec_result[1][0]:
                                try:
                                    SQL_exec_result[1][0][key] = '该键解释为：' + str(data_index_annotation[key]) + '\n' + '值示例：由于你经常把值示例当成主体信息，此处不提供任何值示例，只给你键的解释。' 
                                except KeyError:
                                    pass
                            SQL_exec_result = f"以下是执行{result['sql_command']}的结果，下述内容为样例，请勿与实际需要查询的主体搞混，仅作为后续查询的参考格式：\n" + json.dumps(SQL_exec_result[1][0], indent=2, ensure_ascii=False)
                            
                    else:
                        data = {
                                "sql": result['sql_command'],
                                "limit": 10000
                        }
                        specific_logger.info('正在执行sql命令：' + result['sql_command'])
                        SQL_exec_result = self.sql_execute_node(data)
                        
                        if SQL_exec_result[2] is not None:
                            if SQL_exec_result[2] > 5 and len(result['sql_command'].split(' ')) == 4:
                                specific_logger.info('查询样例时没使用LIMIT 1。')
                                messages.append({"role": "assistant", "content": result_raw})
                                messages.append({"role": "user", "content": f"查询样例时请务必使用LIMIT 1，否则会出来很多结果，污染上下文。"})
                                continue
                        
                        if SQL_exec_result[0] == '0':
                            specific_logger.info('查询失败')
                            messages.append({"role": "assistant", "content": result_raw})
                            messages.append({"role": "user", "content": f"执行{result['sql_command']}查询失败，失败原因如下：\n" + json.dumps(SQL_exec_result[1], indent=2, ensure_ascii=False) + "\n请你使用其他方式查询。"})
                            num_kill += 1
                            if num_kill == 5:
                                messages = [
                                    {"role": "system", "content": prompt_search_unicode},
                                    {"role": "user", "content": '请你按照系统提示词要求的完成以下实体的相关内容的查询：\n'+ entity + '\n请你先按照系统提示词，查询相关的库表的样例库'}
                                ]
                            continue
                        else:
                            specific_logger.info('查询成功，即将进入下一个交互中')
                            SQL_exec_result = f"以下是执行{result['sql_command']}的结果：\n" + json.dumps(SQL_exec_result[1], indent=2, ensure_ascii=False)
                            
                    messages.append({"role": "assistant", "content": result_raw})
                    messages.append({"role": "user", "content": SQL_exec_result + '\n' + "请你根据上述查询结果以确定下一步回答内容和查询方式（若还需要进一步查询）。如果你已查询出相关的关联代码，请勿继续查询，将你查询到的关联代码信息输出到answer中并将sql_command置为空。"})
                
            except Exception as e:
                specific_logger.info('当前格式不满足json要求，请重新生成')
                specific_logger.info(str(e))
                specific_logger.info('原始输出为：\n' + result_raw)
                messages.append({"role": "assistant", "content": result_raw})
                messages.append({"role": "user", "content": '你的输出不满足JSON格式要求，请重新生成'})
                continue
            
    def entity_extract(self, query):
        messages = [
            {"role": "system", "content": prompt_entity_extract},
            {"role": "user", "content": query}
        ]
        result = self.model_call(messages, model_name='glm-4-plus', temperature=0.1, max_tokens=50, time_out=20)
        
        return result
        
    def check_time(self, SQL_command):
        prompt_check_time = "你是一个检查SQL语句中时间格式是否规范，时间格式要求是YYYY-MM-DD HH:MM:SS.mmm。例如：像2021-01-21这样的是不规范的，因为没有后面的HH:MM:SS.mmm，正确的应该是2021-01-21 12:00:00.000。请你注意：有且仅有出现YYYY-MM-DD的格式的时间，而没有后面的HH:MM:SS.mmm你才需要判断是否正确，其他形式的时间，不在你的考虑范畴。\n如果存在时间格式不规范的情况，请你输出：0;如果SQL查询语句中不包含时间或包含的时间格式规范则输出：1。\n请勿输出其他任何内容。"
        messages = [
                {"role": "system", "content": prompt_check_time},
                {"role": "user", "content": '请检查一下SQL语句时间格式是否规范：\n' + SQL_command}
            ]
        while True:
            result = self.model_call(messages, model_name='glm-4-plus', temperature=0.3)

            if result not in ['0', '1']:
                specific_logger.info('检查时间格式未成功')
                specific_logger.info(result)
                messages.append({"role": "assistant", "content": result})
                messages.append({"role": "user", "content": "你的输出不符合要求，请重新生成，请你只输出系统提示词中输出要求只输出0和1"})
                continue
            else:
                return result
            
    
    def plan_node(self, all_example, query):
        database_can_search = '以下是你可以查询的所有库：\n'
        for key in Astock_database_container:
            database_can_search += key + '\n'
        
        all_example = '以下是上一个节点给你提供的有详细解释的库表：\n' + all_example
        
        prompt_qustion_send = database_can_search + '\n' + all_example + '\n' + '以下是用户的问题和历史问答（若有）：\n' + query + '\n' + '请你根据提示词的要求写出当前问题的查询计划。历史问题中已有答案，请你集中注意力解决规划当前问题的查询路径。历史问答的作用是给你获得当前问题中某些代指的具体内容。'
        
        messages = [
                {"role": "system", "content": prompt_plan_search},
                {"role": "user", "content": prompt_qustion_send}
            ]
        
        result = self.model_call(messages, model_name='glm-4-plus', temperature=0.3)
        specific_logger.info("\n\n")
        
        self.logging_color('查询路径为：' + result, '31')
        
        return result + '\n该查询规划中若含有类似2021-01-21这样的时间格式，请勿直接使用该格式，具体格式为：YYYY-MM-DD HH:MM:SS.mmm，像2021-01-21 12:00:00.000才是正确的。\n'
    
    def check_num(self, answer):
        
        messages = [
                {"role": "system", "content": prompt_check_num},
                {"role": "user", "content": '请检查以下用户输入的内容，如果有数据格式错误请修正过来，请你务必识别出来有逗号隔开的数据并删除掉对应的逗号：\n' + answer}
            ]
        while True:
            try:
                result = self.model_call(messages, model_name='glm-4-plus', temperature=0.3)
                return result
            except Exception as e:
                specific_logger.info('检查数据有报错')
                
    def check_search_data(self, answer, query):
        messages = [
                {"role": "system", "content": prompt_check_search_data},
                {"role": "user", "content": '以下是询问的题目：\n' + query + '\n请检查以下Agent回答的内容，如果回答有摆烂风险，则输出0，如果回答没有摆烂风险，则输出1：\n' + answer}
            ]
        specific_logger.info('正在检查是否摆烂')
        num = 0
        while num < 3:
            num += 1
            result = self.model_call(messages, model_name='glm-4-plus', temperature=0.1)
            
            if result in ['0', '1']:
                return result
            else:
                messages.append({"role": "assistant", "content": result})
                messages.append({"role": "user", "content": "你的输出不符合要求，请重新生成，请你只输出系统提示词中输出要求只输出0和1"})
                specific_logger.info('检查是否摆烂发生输出错误。')
                continue
        return '1'
    
    def exp_node(self, query_raw):
        num = 1
        query = query_raw
        for key in prompt_Fallible_point:
            ex = prompt_Fallible_point[key]
            messages = [
                {"role": "system", "content": prompt_tips},
                {"role": "user", "content": '题目：' + query_raw + '\n经验：' + ex + '\n请根据系统提示词做出你的判断，输出只能为0和1.'}
            ]
            resutl = self.model_call(messages, model_name='glm-4-plus', temperature=0.1)
            if "1" in resutl:
                query += f"\n查询经验{num}：{ex}"
                num += 1
        return query
    
    def build_relation_node(self, query_raw):
        messages = [
                {"role": "system", "content": self.prompt_build_relation_node},
                {"role": "user", "content": query_raw + '\n请按照系统提示词输出相关判断内容'}
            ]
        num = 0
        while num < 3:
            result = self.model_call(messages, model_name='glm-4-plus', temperature=0.1, max_tokens=50)
            
            if result == '0':
                return None
            elif "当前问题查询的约束范围" not in result:
                messages = [
                    {"role": "system", "content": self.prompt_build_relation_node},
                    {"role": "user", "content": query_raw + "\n请按照系统提示词输出相关判断内容，请注意你的输出部分要含有'当前问题查询的约束范围'"}
                ]
                num += 1
                continue
            else:
                return result
            
    def check_answer_node(self, query, answer):
        messages = [
                {"role": "system", "content": self.prompt_check_answer_node},
                {"role": "user", "content": '问题：' + query + '\nagent回答：' + answer + '\n请你按照系统提示词要求完成相应的输出'}
            ]
        
        result = self.model_call(messages, model_name='glm-4-plus', temperature=0.1)
        
        messages = [
                {"role": "system", "content": self.prompt_check_answer_node_child},
                {"role": "user", "content": '问题：' + query + '\nagent回答：' + result + '\n请你按照系统提示词要求完成相应的输出。'}
            ]
        
        result = self.model_call(messages, model_name='glm-4-plus', temperature=0.1)
        return result
    
    def InfoSource_check(self, SQL_command):        
        tips_infosource = ""
        for key in self.libraries_infosource:
            if key in SQL_command:
                if self.libraries_infosource[key] == 0 and f"{key}中的InfoSource不能使用像'年度报告'，'半年报'，'季度报告'，'2020'这样的值进行查询，如果没有题目没有明确说明查询该表的信息来源，查询该表无需使用InfoSource进行约束。请你仔细检查你的SQL语句中有没有犯这个错误，如果有请修正过来。\n" not in tips_infosource:
                    tips_infosource += f"{key}中的InfoSource不能使用像'年度报告'，'半年报'，'季度报告'，'2020'这样的值进行查询，如果没有题目没有明确说明查询该表的信息来源，查询该表无需使用InfoSource进行约束。请你仔细检查你的SQL语句中有没有犯这个错误，如果有请修正过来。\n"
                elif f"{key}中的InfoSource键对应的财务报告类型只有这几种：{self.libraries_infosource[key]}，查询财务报告时只能使用这范围内的名称，请勿加其他任何修饰词。若题目需要查询其他类型报告，例如上市公告书，则以题目要求为准，这种情况下InfoSource LIKE %上市公告书%\n" not in tips_infosource:
                    tips_infosource += f"{key}中的InfoSource键对应的财务报告类型只有这几种：{self.libraries_infosource[key]}，查询财务报告时只能使用这范围内的名称，请勿加其他任何修饰词。若题目需要查询其他类型报告，例如上市公告书，则以题目要求为准，这种情况下InfoSource LIKE %上市公告书%\n"
            else:
                continue
        return tips_infosource
