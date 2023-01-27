# 1 业务背景知识

## （1）银行背景

至少相关的基本架构和运作形式，业务部门（因为FRTB必定会涉及具体的业务线，但不是全部，理解相关业务部门的基本架构有助于理解FRTB中的各个数据来源的含义）

## （2）Trading book 和 bank book（原因如上）

![image-20230127135433654](/Volumes/Disk2/screencaptures/new joiner KT/image-20230127135433654.png)

## （3）Risk（特别是与FRTB相关的Risk）

过去实际的风险案例导致监管要求升级（接上），FRTB的由来

![image-20230127135011773](/Volumes/Disk2/screencaptures/new joiner KT/image-20230127135011773.png)



## （5）解决办法：FRTB相关的理论知识概述（三个关键）

![image-20230127140545659](/Volumes/Disk2/screencaptures/new joiner KT/image-20230127140545659.png)

（解决问题的办法）

- 账簿划分

- IMA

- SA （SBA敏感度资本、DRC违约风险资本和RRAO剩余风险附加）

## （6）RBC在FRTB上的大致要求

（能力，timeline等）：要做哪些功能，输入哪些数据，输出哪些数据（根据前面已经介绍的概念）

# 2 项目相关组织架构

基于前述背景和RBC CS内部组织架构，针对FRTB整个大项目的团队大概构成和工作内容：

## 核心团队

项目管理PM部分

业务分析BA部分

系统开发DEV部分（分了大概几个团队，大概做什么：标准叫法SA，IMA？（内部代号，Darwin，RDL，ATOM？）

测试质量QA部分

部署运维OP部分

大概如何运作（CTB， RTB。。。）

## 辅助支撑

其他平台支撑部分（CDP，CI/CD，SNS等）大概有什么用

流程管理相关（各种审核的）

## 本团队构成

负责内容： FRTB大框架的SA部分

# 3 团队承担的项目或子系统



## 3.1概述

子系统的主要能力概述（EGL，RDL，ReportingServcie，。。。），涉及的业务部门，输入的大致内容，输出的大致内容，处理的简单逻辑

## 3.2 系统技术体系

使用Spring/Springboot 提供 Rest API

使用CDP进行数据处理、存储和管理（大概使用了哪些组件干什么）

- 使用HDFS+Hive存储数据
- 使用Spark处理数据
- 使用Dremio管理数据
- 使用Hue查看数据

**技术架构图**

![img](/Volumes/Disk2/screencaptures/new joiner KT/v2-46f5907b0b9a6b79888d8b2cd3a480f0_1440w.webp)

## 3.3 系统流程架构

数据源和输入形式（接口形式）有哪些：

流入流出逻辑有哪些：简单的高层的内部处理逻辑

数据内容和输出形式（接口形式）有哪些：比如报表，Rest API

## 3.4 系统功能架构 

简单说，系统有哪些模块，分别是干什么的，提供了什么必要的能力（来支持前面概述的业务要求）

![image-20230127142838001](/Volumes/Disk2/screencaptures/new joiner KT/image-20230127142838001.png)

## 3.5 系统部署形式

理论上的部署方式（可以部署一台主机还是几台主机，分别部署什么模块，需要什么支撑平台）

目前实际的部署形式（dev环境，test环境，prod环境）

部署架构图，运作方式（prod环境的Balance，HA（High Availability）或者DR(灾备)）

![部署架构_产品介绍_云数据库RDS_敏捷版数据库场景](/Volumes/Disk2/screencaptures/new joiner KT/p248496.png)

资源列表



## 3.6 主要模块功能

与系统代码结构基本对应

### 3.6.1 rdl-FRTB

#### 3.6.1.1 SBA

需求：

算法：

输入/逻辑/输出

#### 3.6.1.2 DRC

#### 3.6.1.3 RRAO

#### 3.6.1.4 MASTER CRIF

#### 3.6.1.5 POSITION

#### 3.6.1.6 FX

#### 3.6.1.7 WhatIf 

#### 3.6.1.8 Bucketing

#### 3.6.1.9 ...

### 3.6.2 Reporting Service

内容，大致模块功能

### 3.6.3 SA Calculador

内容，大致模块功能

### 3.6.4 EGL

