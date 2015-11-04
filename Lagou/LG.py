#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json
import os, logging
import random

import requests, re
from pyquery import PyQuery
from threading import Thread
from queue import Queue
import time
from db import LGDB

# LG使用的是每个职业的搜索列表里的30*15的最多450条数据，并不完整
class LG:
    def __init__(self, threadNum):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36'}
        self.URL = "http://www.lagou.com/"
        self.position = []  # 使用元祖储存职业名
        self.q_req = Queue()
        self.threadNum = threadNum
        self.lagou_db = LGDB()

    def getPosition(self):
        pageCode = self.getPageCode(self.URL)
        query = PyQuery(pageCode)
        positionData = query(".menu_sub.dn .reset dd a")

        for i in range(positionData.length):
            data = positionData.eq(i)
            name = data.text()
            link = data.attr("href")
            self.position.append((name, link))

        print("获取职业列表成功!")
        return self.position

    def getPageCode(self, url):
        time.sleep(random.randint(0,5))
        try:
            return requests.get(url, headers=self.headers).content.decode('utf-8')
        except Exception:
            print("*******连接有误********")
            return None

    def getJobList(self, kd):
        # 判断职位是否已被记录过
        if self.lagou_db.isRecordJobName(kd):
            print("数据库已记录过", kd)
            return None

        jobsId = []
        for index in range(1, 31):
            data = {'kd': kd, 'pn': index}
            time.sleep(1)
            jsonData = requests.post(
                "http://www.lagou.com/jobs/positionAjax.json?",
                data=data,
                headers=self.headers
            )
            jobs = jsonData.json()["content"]["result"]
            if not len(jobs):
                break
            print("开始获取%s的数据第%d页的%d条数据" % (kd, index, len(jobs)))
            # 遍历数据，并为其加入主键
            for job in jobs:
                # 将主键加入到job中, 并加入到数据库中
                id = job['positionId']
                job["_id"] = id
                job["companyLogo"] = "http://www.lagou.com/" + job["companyLogo"]
                jobsId.append(id)
                self.lagou_db.addJob(job)

        print("%s的职位录入完毕！一共%d条数据" % (kd, len(jobsId)))
        # 若录入完毕后，将其存到数据库中，下一次将不再获取
        self.lagou_db.recordToSave(kd)

    def workingThread(self):
        while True:
            kd = self.q_req.get()
            self.getJobList(kd)
            time.sleep(1)
            self.q_req.task_done()

    def run(self):
        # 先获取职业列表和链接
        position = self.getPosition()
        for name, url in position:
            self.q_req.put(name)

        for i in range(self.threadNum):
            t = Thread(target=self.workingThread)
            t.setDaemon(True)
            t.start()

        self.q_req.join()

# LG2使用的是遍历职业id，若是id不存在数据库，且页面上有相应数据，则保存
class LG2(LG):
    def __init__(self, threadNum, startId=0):
        super().__init__(threadNum)
        self.URL = "http://www.lagou.com/jobs/%d.html"
        self.startId = startId

    def getJobData(self, positionId):
        url = self.URL % positionId
        try:
            pageCode = self.getPageCode(url)
        except ConnectionError:
            logging.error("******连接失败******")
            pageCode = None

        # 无页面返回
        if not pageCode:
            return None

        query = PyQuery(pageCode)
        # 职业已被删除
        if query(".position_del") or query(".wait"):
            self.lagou_db.saveLeftRecord(positionId)
            return None

        jobData = {}

        # 公司信息
        companyInformation = query(".job_company")
        jobData["companyName"] = companyInformation.find('dt h2.fl').contents()[0].strip()
        jobData["companyShortName"] = companyInformation.find('dt img').attr('alt')
        jobData["companyLogo"] = companyInformation.find('dt img').attr('src')
        jobData["financeStage"] = companyInformation.find(".c_feature.reset").eq(1).find("li").contents()[1].strip()
        jobData["companyId"] = int(re.findall("\d+", companyInformation.find('dt a').attr('href'))[0])
        info = companyInformation.find(".c_feature.reset:first-child li")
        jobData["industryField"] = info.eq(0).contents()[-1].strip()
        jobData["companySize"] = info.eq(1).contents()[-1].strip()
        jobData["website"] = info.eq(2).find("a").text()

        # 职位信息
        jobInformation = query(".job_detail")
        jobData["_id"] = positionId
        jobData["positionId"] = positionId
        jobData["positionName"] = jobInformation.find("dt h1").attr("title")
        info = jobInformation.find("dd.job_request span")
        jobData["salary"] = info.eq(0).text()
        jobData["city"] = info.eq(1).text()
        jobData["workYear"] = info.eq(2).text()
        # 捕获学历
        matchArr = re.findall("(初中|高中|大学|中专|大专|本科|研究生|硕士|博士)", info.eq(3).text())
        try:
            jobData["education"] = matchArr[0]
        except IndexError:
            jobData["education"] = info.eq(3).text()
        jobData["jobNature"] = info.eq(4).text()
        # 捕获发布时间
        matchArr = re.findall("\d{4}-\d{1,2}-\d{1,2}", jobInformation.find("dd.job_request div").text())
        if len(matchArr) > 0:
            jobData["formatCreateTime"] = matchArr[0]
        else:
            jobData["formatCreateTime"] = jobInformation.find("dd.job_request div").text()
        # 捕获薪酬福利
        str = jobInformation.find("dd.job_request").contents()[-3].strip()
        index = str.find(":")
        labelArr = re.findall("\w+", str[index+2:])
        jobData["companyLabelList"] = labelArr

        self.lagou_db.addJob(jobData)
        print("------------>已录入id为%d的职位" % positionId)

    def workingThread(self):
        while True:
            positionId = self.q_req.get()
            # 该职业id是否已存在数据库中，避免不必要的访问
            if self.lagou_db.isRecordJob(positionId):
                print("Warning:", positionId, "已在【数据库】中，不再记录")
            elif self.lagou_db.isLeftPosition(positionId):
                print("Warning:", positionId, "已在【下架列表】中，不再记录")
            else:
                self.getJobData(positionId)
            time.sleep(random.random())
            self.q_req.task_done()

    def run(self):
        # 将id推入队列中，并开启多线程
        print("正在准备id序列中...")
        maxSize = 10000000
        for id in range(self.startId, maxSize):
            percent = (id / maxSize)*100
            if(percent % 10 == 0):
                os.write(1,bytes('\r已完成{}%'.format(int(percent)), 'utf-8'))
            self.q_req.put(id)
        print("100%, id队列准备就绪!")
        for i in range(self.threadNum):
            t = Thread(target=self.workingThread)
            t.setDaemon(True)
            t.start()

        self.q_req.join()


