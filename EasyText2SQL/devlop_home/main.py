import os
import sys
import copy
import json
import time
import signal
import random
from tqdm import tqdm
from configs.api import sql_api_key
from configs.prompt_config import *
from agent_nodes.agent_node import *
from configs.sql_relation_config import *

all_agent_node = all_agent_node()

url = "https://comm.chatglm.cn/finglm2/api/query"

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {sql_api_key}"
}

Astock_data_relation_str = json.dumps(
    Astock_data_relation, ensure_ascii=False, indent=4)

libraries = []

for key in sql_data_head:
    libraries.append(key)

class QuestionProcessor:
    def __init__(self, question_file_path, answer_file_path):
        self.question_file_path = question_file_path
        self.question_dict = self.load_questions()
        self.answer_file_path = answer_file_path
        self.question_num_list = [_ for _ in range(len(self.question_dict))]
        self.waiting_list = []

    def load_questions(self):
        with open(self.question_file_path, "r", encoding="utf-8") as f:
            question_dict_load = json.load(f)
            for q in question_dict_load:
                for q_team in q['team']:
                    q_team['answer'] = ""
            return question_dict_load

    def save_questions(self, answer_file_path_save):
        with open(answer_file_path_save, "w", encoding="utf-8") as f:
            json.dump(self.question_dict, f, ensure_ascii=False, indent=4)

    def process_questions(self):
        for q_index in tqdm(self.question_num_list, desc=f'当前处理任务进度条'):
            question_list = self.question_dict[q_index]
            try:
                num_kill = 0
                num_kill_libraries = 0
                flag = 0
                while num_kill < 2:
                    num_kill += 1 # 防止无限循环
                    question_team = question_list["team"]
                    query = question_team[0]['question']
                    specific_logger.info('当前问题是：' + query)
                    entity = all_agent_node.entity_extract(query)
                    market = ''
                    if '没有需要抽取的实体' not in entity:
                        try:
                            if ',' in entity:
                                entity_list = [item.strip() for item in entity.split(',')]
                            elif '，' in entity:
                                entity_list = [item.strip() for item in entity.split('，')]
                            else:
                                entity_list = [entity.strip()]
                            entity_list_post = []
                            for e in entity_list:
                                if e in query:
                                    entity_list_post.append(e)
                            specific_logger.info("分割出来的实体为：" + str(entity_list_post))
                            tips, market = all_agent_node.search_unicode_node_pre(entity_list_post)
                            specific_logger.info(tips)
                        except Exception as e:
                            print('分割出现错误' + str(e))  # 预防潜在错误
                            tips = ''
                        if tips != '':
                            tips += f"\n{market}"
                        search_unicode_answer = all_agent_node.search_unicode_node(entity, tips)
                        if 'XXX' in search_unicode_answer or 'XX' in search_unicode_answer:
                            search_unicode_answer = "暂无前置信息"
                        specific_logger.info(search_unicode_answer)
                    else:
                        search_unicode_answer = ''
                        specific_logger.info('没有需要抽取的实体\n\n')
                    all_agent_node.memory = {}  # 重置memory
                    queries = ''
                    num = 1
                    query_list = []
                    relation_query = ""
                    for n, q in enumerate(question_team):
                        q_copy = copy.deepcopy(q)
                        try:
                            if market != '' and n == 0:
                                q_copy['question'] += market
                        except Exception as e:
                            print('market出现错误' + str(e))
                        relation_query += f"问题{n + 1}: " + q['question'] + '\n'
                        if n > 0:
                            relation = all_agent_node.build_relation_node(relation_query)
                            if relation is not None:
                                q_copy['question'] += f'（请你特别注意：{relation} 请勿漏掉该约束条件）'
                        if any(y in q['question'] for y in ['2018', '2019', '2020', '2021', '2022', '2023']):
                            q_copy['question'] += '（请注意：当前题目有对年份的限定，查询相关内容是请勿漏掉年份限定。否则会查询错误。）'
                        q_copy['question'] = all_agent_node.exp_node(q_copy['question'])
                        if '分红收益' in q['question']:
                            q_copy['question'] += '\n请注意：计算分红收益的公式为：(持有份额 / 10) * 派现比例，请严格按照此公式进行计算，一定要先将份额除以10，否则会计算错误，比如说派现比例是0.052，则1000份的收益是(1000/10)*0.052 = 5.2元。'
                        if '与' in q['question'] and '各有' in q['question']:
                            q_copy['question'] += '\n请注意：请注意区分清楚条件是并列关系还是独立关系。例如满足条件A与满足条件B地股票各有多少个，则说明需要分别查询满足条件A，满足条件B的股票数量。'
                        if '合并' in q['question'] or '母公司' in q['question']:
                            q_copy['question'] += "\n请注意：当前题目有对查询报表的主体有限定，请使用IfMerged来约束，例如IfMerged = '1'是指合并，例如IfMerged = '2'是指母公司。如果查询的表中有IfAdjusted，默认设置为2。"
                        if q['question'].count('？') > 1:
                            q_copy['question'] += "\n特别强调：当前问题有多个子问题，请勿把将子问题的查询条件搞混，否则会查询错误。如果你能很好的区分清楚子问题之间的查询条件，将会被智谱公司奖励1000万美元。"
                        queries += f'问题{num}:' + q_copy['question'] + '\n'
                        if '申万' in queries:
                            q_copy['question'] += "\n此处特别强调：当前题目有申万这个条件，因此需要把信息来源约束为申万研究所。所以在查询astockindustrydb.lc_exgindustry时，请你务必加上这两个约束条件InfoSource='申万研究所'，IfPerformed = '1';在查询astockindustrydb.lc_exgindchange时，请你务必加上这两个约束条件InfoSource='申万研究所'， IfExecuted='1'。"
                        specific_logger.info('拼接经验后的问题为：\n' + q_copy['question'] + '\n\n')
                        query_list.append(q_copy['question'])
                        num += 1
                    libraires_query = '需要查询的库有：'
                    prompt_query_raw = "用户查询的内容为：\n" + queries + '\n'
                    while True:
                        library_name_list = []
                        num_library = 0
                        for key in Astock_data_relation:
                            libraries_explain = f"{key}: {Astock_data_relation[key]}"
                            prompt_query = prompt_query_raw + "当前判断是否需要查询的库表为：\n" + libraries_explain
                            messages = [
                                {'role': 'system', 'content': prompt_library_choose_only_0_1},
                                {'role': 'user', 'content': prompt_query}
                            ]
                            result = all_agent_node.model_call(messages)
                            if '1' in result:
                                library_name_list.append(libraries[num_library])
                            num_library += 1
                        if len(library_name_list) > 15:
                            library_name_list = library_name_list[:15]
                        specific_logger.info(library_name_list)
                        if any(library_name not in sql_data_head for library_name in library_name_list):
                            specific_logger.info('输出的库是不正确的，重新查询')
                            specific_logger.info(library_name_list)
                            num_kill_libraries += 1
                            if num_kill_libraries == 5:
                                flag = 1
                                break
                        else:
                            specific_logger.info('所需查询的库有：' + str(library_name_list))
                            break
                    if flag == 1:
                        continue
                    specific_logger.info('正在查询所有样本库结构')
                    all_example, library_container = all_agent_node.search_all_sample(library_name_list)
                    search_path = all_agent_node.plan_node(all_example, f'前置信息为：{search_unicode_answer}\n上述前置信息是有扩大模糊查询得来，信息不一定准确，但有一定的参考作用。因此需要结合题设和前置信息，确保使用的前置信息是跟题目相关联的。\n\n当前问题为：' + query_list[0] + '\n此时为第一问，没有历史问答。')
                    all_agent_node.key_name_container = {}
                    query_answer = all_agent_node.recursion_search_node(all_example, query_list, search_unicode_answer, library_name_list, search_path)
                    if query_answer is None:
                        continue
                    if len(query_answer) == 0:
                        continue
                    continue_flag = False # 用于防止单位错误
                    try:
                        for i, q in enumerate(question_team):
                            if i < len(query_answer):
                                query_answer[i] = all_agent_node.check_answer_node(q['question'], query_answer[i]) if query_answer[i] != '' else query_answer[i] # 检测答案是否满足题目要求
                            else:
                                query_answer.append('') # 如果题目数量比答案数量多，则将答案设置为空
                            q['answer'] = query_answer[i]
                            if '亿' in q['answer'] and '亿' not in q['question']:
                                continue_flag = True
                    except Exception as e:
                        print('序号错误')
                    self.save_questions(self.answer_file_path)
                    ansewer_repeat = []
                    if continue_flag == True and num_kill < 2:
                        specific_logger.info('单位错误，重新生成答案')
                        continue
                    if any(len(m) == 0 for m in query_answer) and num_kill < 2:
                        specific_logger.info('重新生成答案')
                        continue
                    else:
                        specific_logger.info('答案生成成功\n\n\n')
                        print(f'\n第{q_index + 1}题答案为：\n')
                        print(query_answer)
                        if (any('无法' in answer for answer in query_answer) or any('抱歉' in answer for answer in query_answer)) and q_index not in self.waiting_list:
                            self.waiting_list.append(q_index) # 答案生成成功后，将无法回答的问题加入等待列表
                        break
            except Exception as e:
                specific_logger.info('发生错误，错误为：' + str(e))
                print('\n发生错误，错误为：' + str(e) + '\n')
                continue

def read_answers_from_files(file_paths):
    all_answers = {}
    for file_path in file_paths:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            for item in data:
                tid = item['tid']
                if tid not in all_answers:
                    all_answers[tid] = {}
                for question in item['team']:
                    qid = question['id']
                    if qid not in all_answers[tid]:
                        all_answers[tid][qid] = {
                            'question': question['question'],
                            'answers': []
                        }
                    if question['answer'] not in all_answers[tid][qid]['answers']:
                        all_answers[tid][qid]['answers'].append(question['answer'])
    return all_answers

def merge_answers_with_model(all_answers, model_call):
    merged_answers = {}
    for tid, questions in all_answers.items():
        merged_answers[tid] = []
        for qid, data in questions.items():
            
            prompt = prompt_merge_answers.format(question=data['question'], answers=data['answers'])
            messages = [
                {'role': 'system', 'content': "你是一个智能助手，负责融合多个答案。"},
                {'role': 'user', 'content': prompt}
            ]
            merged_answer = model_call(messages)
            merged_answers[tid].append({
                'id': qid,
                'question': data['question'],
                'answer': merged_answer
            })
    return merged_answers

def save_merged_answers(file_paths, merged_answers):
    for file_path in file_paths:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            for item in data:
                tid = item['tid']
                for i, question in enumerate(item['team']):
                    qid = question['id']
                    for merged_answer in merged_answers[tid]:
                        if merged_answer['id'] == qid:
                            question['answer'] = merged_answer['answer']
                            break
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

def process_with_delay(question_file_path, delay):
    processor = QuestionProcessor(question_file_path)
    processor.process_questions()
    time.sleep(delay)

if __name__ == "__main__":
    def timeout_handler(signum, frame):
        raise TimeoutError("脚本运行时间超过限制")

    # 设置信号处理函数
    signal.signal(signal.SIGALRM, timeout_handler)

    # 设置超时时间（以秒为单位）
    timeout_duration = 86400

    if len(sys.argv) < 2: 
        question_file = '../devlop_data/question.json'
        answer_file = '../devlop_result/answer.json'
    else:
        question_file = sys.argv[1]
        answer_file = sys.argv[2]
        
    question_file_paths = [
        question_file,
    ]
    
    main_process = QuestionProcessor(question_file_paths[0], answer_file)
    
    query_index_all = [_ for _ in range(len(main_process.question_dict))]
    random.shuffle(query_index_all)
    
    main_process.question_num_list = query_index_all
    try:
        signal.alarm(timeout_duration)

        main_process.process_questions()
        
        waiting_list = copy.deepcopy(main_process.waiting_list)
        
        if len(waiting_list) != 0:
            waiting_list = waiting_list[:20] if len(waiting_list) > 20 else waiting_list
            main_process.question_num_list = waiting_list
            main_process.waiting_list = []
            main_process.process_questions()

    except TimeoutError as e:
        print(str(e))

    finally:
        signal.alarm(0)