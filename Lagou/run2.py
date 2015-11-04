#!/usr/bin/env python
# -*- coding:utf-8 -*-
from LG import LG2
if __name__ == '__main__':
    print("使用第二种方式爬取数据...")
    lg = LG2(10, 370000)
    lg.run()
    print("所有数据已存储...")
