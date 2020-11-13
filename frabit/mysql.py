# (c) 2020 Frabit Project maintained and limited by Blylei < blylei918@gmail.com >
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
#
# This file is part of Frabit
#
"""
基于mysql_connector封装对MySQL的操作接口
"""
import os
import sys
import re
import datetime
import time

import frabit
from frabit import utils
from frabit import exceptions
from frabit import output
import _mysql_connector 