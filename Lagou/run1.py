#!/usr/bin/env python
# -*- coding:utf-8 -*-
from Lagou.LG import LG
if __name__ == '__main__':
    print("使用第一种方式爬取数据...")
    lg = LG(10)
    lg.run()
    print("所有数据已存储...")