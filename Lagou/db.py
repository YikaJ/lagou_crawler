#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pymongo import MongoClient

# 连接到数据库并切换到lagou数据库
class LGDB:
    def __init__(self):
        connection = MongoClient('localhost', 27017)
        print("连接数据库成功!")
        self.db = connection.lagou
        
    def addJob(self, job):
        try:
            self.db.jobs.insert_one(job)
        except Exception as err:
            print(err)

    # 当完成一个职位的提取后，将其存到数据库中
    def recordToSave(self, jobName):
        self.db.alreayRecord.insert_one({"kd": jobName})
        print("已将%s存到已记录列表中" % jobName)

    # 存储已下架的id数据
    def saveLeftRecord(self, positionId):
        self.db.alreayRecord.insert_one({"_id": positionId})
        print("Error: 职位已经下架, 已将下架id%d存到记录列表中" % positionId)

    # return Boolean
    def isRecordJobName(self, jobName):
        "判断需要搜索的职业名称是否在已存数据中"
        record = self.db.alreayRecord.find_one({"kd": jobName})
        if record:
            return True
        else:
            return False

    # return Boolean
    def isLeftPosition(self, positionId):
        "判断需要搜索的Id是否在下架列表中"
        record = self.db.alreayRecord.find_one({"_id": positionId})
        if record:
            return True
        else:
            return False

    # return Boolean
    def isRecordJob(self, positionId):
        "判断_id是否已存在"
        record = self.db.jobs.find_one({"_id": positionId})
        if record:
            return True
        else:
            return False