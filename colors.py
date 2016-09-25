#!/usr/bin/env python
# encoding:utf-8
# colors.py
# 2016/8/31  20:05

"""
copy from fabric/colors.py
"""

import os,time

class Logger(object):
    LOG_LEVEL_RECORD    = 100

    LOG_LEVEL_ERROR     = 99
    LOG_LEVEL_DEBUG     = 50
    LOG_LEVEL_VERBOSE   = 1

    LOG_LEVEL_SILENCE   = 0

    def __init__(self, cmd_mode=True, file_mode=False, db_mode=False, filename=None, level=LOG_LEVEL_VERBOSE):
        if not filename:
            filename = time.strftime('%y%m%d.log')

        self.filename   = filename
        self.level      = level
        self.cmd_mode   = cmd_mode
        self.file_mode  = file_mode
        self.db_mode    = db_mode

    # 输出到命令行
    def _cmd_log(self, text):
        print text

    # 输出到日志文件
    def _file_log(self, text):
        pass

    # 保存到数据库中（供后续web展示）
    def _db_log(self, text):
        pass

    def _record(self, text, level):
        if level < self.level:
            return

        self.cmd_mode   and self._cmd_log(text)
        self.file_mode  and self._file_log(text)
        self.db_mode    and self._db_log(text)

    def error(self, text):
        self._record(text, Logger.LOG_LEVEL_ERROR)

    def debug(self, text):
        self._record(text, Logger.LOG_LEVEL_DEBUG)

    def verbose(self, text):
        self._record(text, Logger.LOG_LEVEL_VERBOSE)

    def dblog(self, text):
        self._record(text, Logger.LOG_LEVEL_RECORD)



def _wrap_with(code):

    def inner(text, bold=True):
        c = code

        if os.environ.get('FABRIC_DISABLE_COLORS'):
            return text

        if bold:
            c = "1;%s" % c
        return "\033[%sm%s\033[0m" % (c, text)
    return inner

red = _wrap_with('31')
green = _wrap_with('32')
yellow = _wrap_with('33')
blue = _wrap_with('34')
magenta = _wrap_with('35')
cyan = _wrap_with('36')
white = _wrap_with('37')
