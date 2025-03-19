# Chilean Education System for School Absences and Early Truancy

## General Description

This document provides an overview of three key datasets related to the Chilean education system, focusing on student attendance and academic performance in various programs and schools registered with the Chilean Ministry of Education. These datasets allow for the analysis of educational trends and the impact of the COVID-19 pandemic.

The datasets include:

1. **Kinder**: Annual attendance data for children in early education programs.
2. **Assistance**: Monthly attendance data for students in schools.
3. **Performance**: Annual academic performance data of students.

## Dataset Details

### 1. Kinder
- Records the yearly attendance of children enrolled in three programs:
  1. **Sistema de Información General de Estudiantes (SIGE)**
  2. **Sistema de Captura JUNJI**
  3. **Sistema de Captura INTEGRA**
- Covered period: **2011 - 2023** (including the pandemic).
- Total observations: **9,879,622**.

### 2. Assistance
- Records the monthly attendance of students enrolled in schools registered with the Chilean Ministry of Education.
- Covered period: **2011 - 2023**, with a gap in **2020 and 2021** due to the pandemic.
- The academic year in Chile consists of **10 months**, running from **March to December**.
- Total observations: **382,055,737**.

### 3. Performance
- Records the yearly academic performance (including attendance) of students enrolled in schools.
- Covered period: **2002 - 2023**.
- Data sources:
  - **Registro de Estudiantes de Chile (RECH)** for **2002 - 2008**.
  - **Sistema de Información General de Estudiantes (SIGE)** for **2009 - 2023**.
- Total observations: **73,668,732**.

## Data Collection and Storage

- The **data scraping and database structuring scripts** are located in the **code folder**.
- The datasets themselves are **not uploaded** due to their **large size**.

## To-Do List
- [ ] Do a master file for data conformation, where it does not repeat every time the working directory. 
- [ ] We need to map geographically the best bus routes from kids house to school with their frequencies.
