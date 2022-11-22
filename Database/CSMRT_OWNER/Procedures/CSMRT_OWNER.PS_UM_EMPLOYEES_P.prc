DROP PROCEDURE CSMRT_OWNER.PS_UM_EMPLOYEES_P
/

--
-- PS_UM_EMPLOYEES_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_UM_EMPLOYEES_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_UM_EMPLOYEES'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_UM_EMPLOYEES', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_UM_EMPLOYEES'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_EMPLOYEES from PeopleSoft table PS_UM_EMPLOYEES.
--
-- V01  SMT-xxxx 08/22/2017,    Jim Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_EMPLOYEES';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN
strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_UM_EMPLOYEES'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncate',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_UM_EMPLOYEES'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'truncate table CSSTG_OWNER.PS_UM_EMPLOYEES';
begin
execute immediate 'truncate table CSSTG_OWNER.PS_UM_EMPLOYEES';
end;


strSqlCommand := 'commit';
commit;

strMessage01    := 'Loading data into CSSTG_OWNER.PS_UM_EMPLOYEES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'Insert into CSSTG_OWNER.PS_UM_EMPLOYEES';
insert /*+ append */  into CSSTG_OWNER.PS_UM_EMPLOYEES
select /*+ full(S) */
        nvl(trim(EMPLID),'-') EMPLID, 
        nvl(EMPL_RCD,0) EMPL_RCD, 
        'HR90' SRC_SYS_ID,
        to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
        nvl(EFFSEQ,0) EFFSEQ, 
        nvl(trim(NID_COUNTRY),'-') NID_COUNTRY, 
        nvl(trim(NATIONAL_ID_TYPE),'-') NATIONAL_ID_TYPE, 
        nvl(trim(NATIONAL_ID),'-') NATIONAL_ID, 
        nvl(trim(NAME),'-') NAME, 
        nvl(trim(NAME_PREFIX),'-') NAME_PREFIX, 
        nvl(trim(PREFERRED_NAME),'-') PREFERRED_NAME, 
        nvl(trim(COUNTRY),'-') COUNTRY, 
        nvl(trim(ADDRESS1),'-') ADDRESS1, 
        nvl(trim(ADDRESS2),'-') ADDRESS2, 
        nvl(trim(ADDRESS3),'-') ADDRESS3, 
        nvl(trim(ADDRESS4),'-') ADDRESS4, 
        nvl(trim(CITY),'-') CITY, 
        nvl(trim(NUM1),'-') NUM1, 
        nvl(trim(NUM2),'-') NUM2, 
        nvl(trim(HOUSE_TYPE),'-') HOUSE_TYPE, 
        nvl(trim(ADDR_FIELD1),'-') ADDR_FIELD1, 
        nvl(trim(ADDR_FIELD2),'-') ADDR_FIELD2, 
        nvl(trim(ADDR_FIELD3),'-') ADDR_FIELD3, 
        nvl(trim(COUNTY),'-') COUNTY, 
        nvl(trim(STATE),'-') STATE, 
        nvl(trim(POSTAL),'-') POSTAL, 
        nvl(trim(GEO_CODE),'-') GEO_CODE, 
        nvl(trim(IN_CITY_LIMIT),'-') IN_CITY_LIMIT, 
        nvl(trim(HOME_PHONE),'-') HOME_PHONE, 
        nvl(trim(PER_STATUS),'-') PER_STATUS, 
        nvl(trim(PER_ORG),'-') PER_ORG, 
        to_date(to_char(case when ORIG_HIRE_DT < '01-JAN-1800' then NULL else ORIG_HIRE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ORIG_HIRE_DT,
        nvl(trim(SEX),'-') SEX, 
        to_date(to_char(case when BIRTHDATE < '01-JAN-1800' then NULL else BIRTHDATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') BIRTHDATE, 
        nvl(trim(BIRTHPLACE),'-') BIRTHPLACE, 
        to_date(to_char(case when DT_OF_DEATH < '01-JAN-1800' then NULL else DT_OF_DEATH end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DT_OF_DEATH, 
        nvl(trim(MAR_STATUS),'-') MAR_STATUS, 
        nvl(trim(FORMER_NAME),'-') FORMER_NAME, 
        nvl(trim(DISABLED),'-') DISABLED, 
        nvl(trim(DISABLED_VET),'-') DISABLED_VET, 
        nvl(trim(MILITARY_STATUS),'-') MILITARY_STATUS, 
        nvl(trim(HIGHEST_EDUC_LVL),'-') HIGHEST_EDUC_LVL, 
        nvl(trim(CITIZENSHIP_STATUS),'-') CITIZENSHIP_STATUS, 
        nvl(trim(US_WORK_ELIGIBILTY),'-') US_WORK_ELIGIBILTY, 
        nvl(BENEFIT_RCD_NBR,0) BENEFIT_RCD_NBR, 
        nvl(trim(HOME_HOST_CLASS),'-') HOME_HOST_CLASS, 
        to_date(to_char(case when HIRE_DT < '01-JAN-1800' then NULL else HIRE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') HIRE_DT, 
        to_date(to_char(case when REHIRE_DT < '01-JAN-1800' then NULL else REHIRE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') REHIRE_DT, 
        to_date(to_char(case when CMPNY_SENIORITY_DT < '01-JAN-1800' then NULL else CMPNY_SENIORITY_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CMPNY_SENIORITY_DT,
        to_date(to_char(case when SERVICE_DT < '01-JAN-1800' then NULL else SERVICE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SERVICE_DT,
        to_date(to_char(case when EXPECTED_RETURN_DT < '01-JAN-1800' then NULL else EXPECTED_RETURN_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EXPECTED_RETURN_DT,
        to_date(to_char(case when TERMINATION_DT < '01-JAN-1800' then NULL else TERMINATION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') TERMINATION_DT,
        to_date(to_char(case when LAST_DATE_WORKED < '01-JAN-1800' then NULL else LAST_DATE_WORKED end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_DATE_WORKED,
        to_date(to_char(case when LAST_INCREASE_DT < '01-JAN-1800' then NULL else LAST_INCREASE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LAST_INCREASE_DT,
        nvl(trim(OWN_5PERCENT_CO),'-') OWN_5PERCENT_CO, 
        nvl(trim(REFERRAL_SOURCE),'-') REFERRAL_SOURCE, 
        nvl(trim(BUSINESS_TITLE),'-') BUSINESS_TITLE, 
        nvl(trim(FT_STUDENT),'-') FT_STUDENT, 
        nvl(trim(REPORTS_TO),'-') REPORTS_TO, 
        nvl(trim(SUPERVISOR_ID),'-') SUPERVISOR_ID, 
        nvl(trim(UNION_CD),'-') UNION_CD, 
        nvl(trim(BARG_UNIT),'-') BARG_UNIT, 
        to_date(to_char(case when UNION_SENIORITY_DT < '01-JAN-1800' then NULL else UNION_SENIORITY_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UNION_SENIORITY_DT,
        to_date(to_char(case when PROBATION_DT < '01-JAN-1800' then NULL else PROBATION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PROBATION_DT,
        nvl(trim(SECURITY_CLEARANCE),'-') SECURITY_CLEARANCE, 
        nvl(trim(WORK_PHONE),'-') WORK_PHONE, 
        nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT, 
        nvl(trim(DEPTID),'-') DEPTID, 
        nvl(trim(JOBCODE),'-') JOBCODE, 
        nvl(trim(POSITION_NBR),'-') POSITION_NBR, 
        nvl(trim(UM_POSN_DESCR_TTL),'-') UM_POSN_DESCR_TTL, 
        nvl(trim(EMPL_STATUS),'-') EMPL_STATUS, 
        nvl(trim(ACTION),'-') ACTION, 
        to_date(to_char(case when ACTION_DT < '01-JAN-1800' then NULL else ACTION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ACTION_DT, 
        nvl(trim(ACTION_REASON),'-') ACTION_REASON, 
        nvl(trim(LOCATION),'-') LOCATION, 
        to_date(to_char(case when JOB_ENTRY_DT < '01-JAN-1800' then NULL else JOB_ENTRY_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') JOB_ENTRY_DT,
        to_date(to_char(case when DEPT_ENTRY_DT < '01-JAN-1800' then NULL else DEPT_ENTRY_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DEPT_ENTRY_DT, 
        to_date(to_char(case when POSITION_ENTRY_DT < '01-JAN-1800' then NULL else POSITION_ENTRY_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') POSITION_ENTRY_DT, 
        nvl(trim(SHIFT),'-') SHIFT, 
        nvl(trim(REG_TEMP),'-') REG_TEMP, 
        nvl(trim(FULL_PART_TIME),'-') FULL_PART_TIME, 
        nvl(trim(FLSA_STATUS),'-') FLSA_STATUS, 
        nvl(trim(OFFICER_CD),'-') OFFICER_CD, 
        nvl(trim(COMPANY),'-') COMPANY, 
        nvl(trim(PAYGROUP),'-') PAYGROUP, 
        nvl(trim(EMPL_TYPE),'-') EMPL_TYPE, 
        nvl(trim(HOLIDAY_SCHEDULE),'-') HOLIDAY_SCHEDULE, 
        nvl(STD_HOURS,0) STD_HOURS, 
        nvl(trim(EEO_CLASS),'-') EEO_CLASS, 
        nvl(trim(SAL_ADMIN_PLAN),'-') SAL_ADMIN_PLAN, 
        nvl(trim(GRADE),'-') GRADE, 
        to_date(to_char(case when GRADE_ENTRY_DT < '01-JAN-1800' then NULL else GRADE_ENTRY_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') GRADE_ENTRY_DT,
        nvl(STEP,0) STEP, 
        to_date(to_char(case when STEP_ENTRY_DT < '01-JAN-1800' then NULL else STEP_ENTRY_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') STEP_ENTRY_DT, 
        nvl(trim(GL_PAY_TYPE),'-') GL_PAY_TYPE, 
        nvl(trim(SALARY_MATRIX_CD),'-') SALARY_MATRIX_CD, 
        nvl(trim(RATING_SCALE),'-') RATING_SCALE, 
        nvl(trim(REVIEW_RATING),'-') REVIEW_RATING, 
        to_date(to_char(case when REVIEW_DT < '01-JAN-1800' then NULL else REVIEW_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') REVIEW_DT, 
        nvl(trim(COMP_FREQUENCY),'-') COMP_FREQUENCY, 
        nvl(COMPRATE,0) COMPRATE, 
        nvl(CHANGE_AMT,0) CHANGE_AMT, 
        nvl(CHANGE_PCT,0) CHANGE_PCT, 
        nvl(ANNUAL_RT,0) ANNUAL_RT, 
        nvl(MONTHLY_RT,0) MONTHLY_RT, 
        nvl(HOURLY_RT,0) HOURLY_RT, 
        nvl(ANNL_BENEF_BASE_RT,0) ANNL_BENEF_BASE_RT, 
        nvl(SHIFT_RT,0) SHIFT_RT, 
        nvl(SHIFT_FACTOR,0) SHIFT_FACTOR, 
        nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
        nvl(trim(JOBTITLE),'-') JOBTITLE, 
        nvl(trim(JOBTITLE_ABBRV),'-') JOBTITLE_ABBRV, 
        nvl(trim(EEO1CODE),'-') EEO1CODE, 
        nvl(trim(EEO4CODE),'-') EEO4CODE, 
        nvl(trim(EEO5CODE),'-') EEO5CODE, 
        nvl(trim(EEO6CODE),'-') EEO6CODE, 
        nvl(trim(EEO_JOB_GROUP),'-') EEO_JOB_GROUP, 
        nvl(trim(JOB_FAMILY),'-') JOB_FAMILY, 
        nvl(JOB_KNOWHOW_POINTS,0) JOB_KNOWHOW_POINTS, 
        nvl(JOB_ACCNTAB_POINTS,0) JOB_ACCNTAB_POINTS, 
        nvl(JOB_PROBSLV_POINTS,0) JOB_PROBSLV_POINTS, 
        nvl(JOB_POINTS_TOTAL,0) JOB_POINTS_TOTAL, 
        nvl(JOB_KNOWHOW_PCT,0) JOB_KNOWHOW_PCT, 
        nvl(JOB_ACCNTAB_PCT,0) JOB_ACCNTAB_PCT, 
        nvl(JOB_PROBSLV_PCT,0) JOB_PROBSLV_PCT, 
        nvl(trim(DEPTNAME),'-') DEPTNAME, 
        nvl(trim(DEPTNAME_ABBRV),'-') DEPTNAME_ABBRV, 
        nvl(trim(MANAGER_ID),'-') MANAGER_ID, 
        nvl(trim(EEO4_FUNCTION),'-') EEO4_FUNCTION, 
        to_date(to_char(case when FROMDATE < '01-JAN-1800' then NULL else FROMDATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') FROMDATE,
        to_date(to_char(case when ASOFDATE < '01-JAN-1800' then NULL else ASOFDATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ASOFDATE,
        nvl(trim(NAME_AC),'-') NAME_AC, 
        nvl(trim(DIRECTLY_TIPPED),'-') DIRECTLY_TIPPED, 
        nvl(trim(IPEDSSCODE),'-') IPEDSSCODE, 
        nvl(trim(VISA_PERMIT_TYPE),'-') VISA_PERMIT_TYPE, 
        nvl(trim(COUNTRY_CD_1042),'-') COUNTRY_CD_1042, 
        nvl(trim(FERPA),'-') FERPA, 
        nvl(FTE,0) FTE, 
        nvl(UM_FTE_RECALC,0) UM_FTE_RECALC, 
        nvl(trim(EMPL_CLASS),'-') EMPL_CLASS, 
        nvl(trim(UM_LEAVE_REASON),'-') UM_LEAVE_REASON, 
        nvl(trim(UM_STATE_TITLE),'-') UM_STATE_TITLE, 
        nvl(UM_VAC_LV_HRS_BAL,0) UM_VAC_LV_HRS_BAL, 
        nvl(UM_SIC_LV_HRS_BAL,0) UM_SIC_LV_HRS_BAL, 
        nvl(UM_ETO_LV_HRS_BAL,0) UM_ETO_LV_HRS_BAL, 
        nvl(UM_CMP_LV_HRS_BAL,0) UM_CMP_LV_HRS_BAL, 
        nvl(UM_OT_CMP_HRS_BAL,0) UM_OT_CMP_HRS_BAL, 
        nvl(UM_WOR_2ND_HRS_BAL,0) UM_WOR_2ND_HRS_BAL, 
        0 UM_WOR_EXT_HRS_BAL, 
        0 UM_WOR_BNK_HRS_BAL, 
        nvl(UM_WOR_RTR_HRS_BAL,0) UM_WOR_RTR_HRS_BAL, 
        0 UM_WOR_VAC_HRS_BAL, 
        nvl(trim(MAIL_DROP),'-') MAIL_DROP, 
        nvl(trim(TASK_PROFILE_ID),'-') TASK_PROFILE_ID, 
        nvl(trim(ERNCD),'-') ERNCD, 
        nvl(trim(UM_CAMPUS_ADDR),'-') UM_CAMPUS_ADDR, 
        nvl(trim(FICA_STATUS_EE),'-') FICA_STATUS_EE, 
        nvl(trim(LAST_NAME),'-') LAST_NAME, 
        nvl(trim(FIRST_NAME),'-') FIRST_NAME, 
        nvl(trim(MIDDLE_NAME),'-') MIDDLE_NAME, 
        nvl(trim(EMAILID),'-') EMAILID, 
        nvl(trim(WORKGROUP),'-') WORKGROUP, 
        to_date(to_char(case when APPOINT_END_DT < '01-JAN-1800' then NULL else APPOINT_END_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') APPOINT_END_DT,
        nvl(UM_PER_LV_HRS_BAL,0) UM_PER_LV_HRS_BAL, 
        nvl(trim(BENEFIT_PROGRAM),'-') BENEFIT_PROGRAM, 
        nvl(trim(SETID_DEPT),'-') SETID_DEPT, 
        nvl(trim(SETID_JOBCODE),'-') SETID_JOBCODE, 
        to_date(to_char(case when EG_TRACK_HIRE_DT < '01-JAN-1800' then NULL else EG_TRACK_HIRE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EG_TRACK_HIRE_DT,
        nvl(trim(EG_ACADEMIC_RANK),'-') EG_ACADEMIC_RANK, 
        nvl(trim(UM_RANK_DESCR),'-') UM_RANK_DESCR, 
        nvl(trim(TENURE_STATUS),'-') TENURE_STATUS, 
        nvl(trim(UM_TEN_STATUS_DESC),'-') UM_TEN_STATUS_DESC, 
        to_date(to_char(case when EG_RNK_CHG_DT < '01-JAN-1800' then NULL else EG_RNK_CHG_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EG_RNK_CHG_DT,         
        nvl(trim(EG_TENURE_HOME),'-') EG_TENURE_HOME, 
        to_date(to_char(case when EG_MAND_REVW_DT < '01-JAN-1800' then NULL else EG_MAND_REVW_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EG_MAND_REVW_DT, 
        to_date(to_char(case when EG_GRANTED_DT < '01-JAN-1800' then NULL else EG_GRANTED_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EG_GRANTED_DT, 
        to_date(to_char(case when EXPECTED_END_DATE < '01-JAN-1800' then NULL else EXPECTED_END_DATE end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EXPECTED_END_DATE, 
        nvl(trim(AUTO_END_FLG),'-') AUTO_END_FLG, 
        nvl(trim(EMPL_CLASS_DESCR),'-') EMPL_CLASS_DESCR, 
        nvl(trim(EXTERNAL_ID_IDM),'-') EXTERNAL_ID_IDM, 
        nvl(trim(EXTERNAL_ID_BDL),'-') EXTERNAL_ID_BDL, 
        nvl(trim(CAMPUS_ID_AMH),'-') CAMPUS_ID_AMH, 
        nvl(trim(SUPERVISOR_NAME),'-') SUPERVISOR_NAME, 
        nvl(trim(REPORTS_TO_EMPLID),'-') REPORTS_TO_EMPLID, 
        nvl(trim(REPORTS_TO_NAME),'-') REPORTS_TO_NAME, 
        nvl(trim(US_SOC_CD),'-') US_SOC_CD, 
        to_date(to_char(case when BENEFIT_PROG_EFFDT < '01-JAN-1800' then NULL else BENEFIT_PROG_EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') BENEFIT_PROG_EFFDT,
        nvl(trim(JOB_IND_DESCR),'-') JOB_IND_DESCR, 
        nvl(UM_PCT_CMP_HRS_BAL,0) UM_PCT_CMP_HRS_BAL, 
        nvl(trim(AA_CENSUS_CD),'-') AA_CENSUS_CD, 
        nvl(BIRTHYEAR,0) BIRTHYEAR,
        'N',
        'S',
        sysdate,
        sysdate,
        1234
 from SYSADM.PS_UM_EMPLOYEES@HRSOURCE R 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_EMPLOYEES rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_EMPLOYEES',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Indexing CSSTG_OWNER.PS_UM_EMPLOYEES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Indexing',
       END_DT = NULL
 where TABLE_NAME = 'PS_UM_EMPLOYEES'
;

strSqlCommand := 'commit';
commit;



strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = sysdate,
       OLD_MAX_SCN = 0,
       NEW_MAX_SCN = 999999999999
 where TABLE_NAME = 'PS_UM_EMPLOYEES'
;


strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END PS_UM_EMPLOYEES_P;
/
