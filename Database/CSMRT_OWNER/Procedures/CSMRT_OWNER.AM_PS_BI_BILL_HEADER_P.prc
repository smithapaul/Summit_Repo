DROP PROCEDURE CSMRT_OWNER.AM_PS_BI_BILL_HEADER_P
/

--
-- AM_PS_BI_BILL_HEADER_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_BI_BILL_HEADER_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_BI_BILL_HEADER from PeopleSoft table PS_BI_BILL_HEADER.
--
-- V01  SMT-xxxx 03/29/2017,    George Adams
--                              Converted from PS_BI_BILL_HEADER.SQL
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_BI_BILL_HEADER';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_BI_BILL_HEADER'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_BI_BILL_HEADER@AMSOURCE S)
 where TABLE_NAME = 'PS_BI_BILL_HEADER'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Merging data into AMSTG_OWNER.PS_BI_BILL_HEADER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_BI_BILL_HEADER';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_BI_BILL_HEADER T 
    using (select /*+ full(S) */
    nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT, 
    nvl(trim(INVOICE_ID),'-') INVOICE_ID, 
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(trim(ADDRESS_TYPE),'-') ADDRESS_TYPE, 
    nvl(trim(BILL_TYPE_ID),'-') BILL_TYPE_ID, 
    nvl(trim(PRIOR_INVOICE_ID),'-') PRIOR_INVOICE_ID, 
    nvl(trim(INVOICE_TYPE),'-') INVOICE_TYPE, 
    nvl(trim(BILL_REQ_ID),'-') BILL_REQ_ID, 
    nvl(BI_REQ_NBR,0) BI_REQ_NBR, 
    to_date(to_char(case when BILL_DATE_TIME < '01-JAN-1800' then NULL 
                    else BILL_DATE_TIME end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') BILL_DATE_TIME,
    to_date(to_char(case when DUE_DT < '01-JAN-1800' then NULL 
                    else DUE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DUE_DT,
    nvl(trim(OPRID),'-') OPRID, 
    nvl(TOTAL_BILL,0) TOTAL_BILL, 
    nvl(trim(SF_BILL_STATUS),'-') SF_BILL_STATUS, 
    nvl(trim(NAME),'-') NAME, 
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
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(ACAD_CAREER_SRCH),'-') ACAD_CAREER_SRCH, 
    nvl(trim(ACAD_CAREER_INC),'-') ACAD_CAREER_INC, 
    nvl(trim(ACAD_PROG_PRIMARY),'-') ACAD_PROG_PRIMARY, 
    nvl(trim(ACAD_PROG_SRCH),'-') ACAD_PROG_SRCH, 
    nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
    nvl(trim(ACAD_LEVEL_PROJ),'-') ACAD_LEVEL_PROJ, 
    nvl(trim(ACAD_LVL_PROJ_SRCH),'-') ACAD_LVL_PROJ_SRCH, 
    nvl(trim(ACAD_LEVEL_PROJ_CL),'-') ACAD_LEVEL_PROJ_CL, 
    nvl(trim(ACAD_PLAN),'-') ACAD_PLAN, 
    nvl(trim(ACAD_PLAN_SRCH),'-') ACAD_PLAN_SRCH, 
    nvl(trim(ACAD_PLAN_INC),'-') ACAD_PLAN_INC, 
    nvl(trim(ACADEMIC_LOAD),'-') ACADEMIC_LOAD, 
    nvl(trim(TUITION_RES),'-') TUITION_RES, 
    nvl(trim(PRT_BILL_FLAG),'-') PRT_BILL_FLAG, 
    nvl(trim(RE_PRT_BILL_FLAG),'-') RE_PRT_BILL_FLAG, 
    to_date(to_char(case when PRT_DTTM_STAMP < '01-JAN-1800' then NULL 
                    else PRT_DTTM_STAMP end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') PRT_DTTM_STAMP,
    to_date(to_char(case when RE_PRT_DTTM_STAMP < '01-JAN-1800' then NULL 
                    else RE_PRT_DTTM_STAMP end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') RE_PRT_DTTM_STAMP, 
    to_date(to_char(case when INVOICE_DT < '01-JAN-1800' then NULL 
                    else INVOICE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') INVOICE_DT,
    nvl(PRIOR_IVC_BALANCE,0) PRIOR_IVC_BALANCE, 
    nvl(MESSAGE_NBR,0) MESSAGE_NBR, 
    nvl(trim(SSF_ERROR_WARN),'-') SSF_ERROR_WARN, 
    nvl(trim(CONTACT_NAME),'-') CONTACT_NAME, 
    nvl(trim(TYPE_OF_REQUEST),'-') TYPE_OF_REQUEST, 
    nvl(trim(BILL_BY_IND),'-') BILL_BY_IND, 
    nvl(trim(BI_SCAN_LINE),'-') BI_SCAN_LINE, 
    nvl(trim(CONTRACT_NUM),'-') CONTRACT_NUM, 
    nvl(trim(CONTRACT_EMPLID),'-') CONTRACT_EMPLID, 
    nvl(trim(CAMPUS),'-') CAMPUS, 
    nvl(trim(SSF_LATEFEE_POST),'-') SSF_LATEFEE_POST, 
    nvl(trim(SSF_CR_INV_IND),'-') SSF_CR_INV_IND, 
    nvl(trim(SCC_ROW_ADD_OPRID),'-') SCC_ROW_ADD_OPRID, 
    to_date(to_char(case when SCC_ROW_ADD_DTTM < '01-JAN-1800' then NULL 
                    else SCC_ROW_ADD_DTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SCC_ROW_ADD_DTTM,
    nvl(trim(SCC_ROW_UPD_OPRID),'-') SCC_ROW_UPD_OPRID, 
    to_date(to_char(case when SCC_ROW_UPD_DTTM < '01-JAN-1800' then NULL 
               else SCC_ROW_UPD_DTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SCC_ROW_UPD_DTTM
  from SYSADM.PS_BI_BILL_HEADER@AMSOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_BI_BILL_HEADER')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S 
 on ( 
    T.BUSINESS_UNIT = S.BUSINESS_UNIT and 
    T.INVOICE_ID = S.INVOICE_ID and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EMPLID = S.EMPLID,
    T.EXT_ORG_ID = S.EXT_ORG_ID,
    T.ADDRESS_TYPE = S.ADDRESS_TYPE,
    T.BILL_TYPE_ID = S.BILL_TYPE_ID,
    T.PRIOR_INVOICE_ID = S.PRIOR_INVOICE_ID,
    T.INVOICE_TYPE = S.INVOICE_TYPE,
    T.BILL_REQ_ID = S.BILL_REQ_ID,
    T.BI_REQ_NBR = S.BI_REQ_NBR,
    T.BILL_DATE_TIME = S.BILL_DATE_TIME,
    T.DUE_DT = S.DUE_DT,
    T.OPRID = S.OPRID,
    T.TOTAL_BILL = S.TOTAL_BILL,
    T.SF_BILL_STATUS = S.SF_BILL_STATUS,
    T.NAME = S.NAME,
    T.COUNTRY = S.COUNTRY,
    T.ADDRESS1 = S.ADDRESS1,
    T.ADDRESS2 = S.ADDRESS2,
    T.ADDRESS3 = S.ADDRESS3,
    T.ADDRESS4 = S.ADDRESS4,
    T.CITY = S.CITY,
    T.NUM1 = S.NUM1,
    T.NUM2 = S.NUM2,
    T.HOUSE_TYPE = S.HOUSE_TYPE,
    T.ADDR_FIELD1 = S.ADDR_FIELD1,
    T.ADDR_FIELD2 = S.ADDR_FIELD2,
    T.ADDR_FIELD3 = S.ADDR_FIELD3,
    T.COUNTY = S.COUNTY,
    T.STATE = S.STATE,
    T.POSTAL = S.POSTAL,
    T.GEO_CODE = S.GEO_CODE,
    T.IN_CITY_LIMIT = S.IN_CITY_LIMIT,
    T.ACAD_CAREER = S.ACAD_CAREER,
    T.ACAD_CAREER_SRCH = S.ACAD_CAREER_SRCH,
    T.ACAD_CAREER_INC = S.ACAD_CAREER_INC,
    T.ACAD_PROG_PRIMARY = S.ACAD_PROG_PRIMARY,
    T.ACAD_PROG_SRCH = S.ACAD_PROG_SRCH,
    T.ACAD_PROG = S.ACAD_PROG,
    T.ACAD_LEVEL_PROJ = S.ACAD_LEVEL_PROJ,
    T.ACAD_LVL_PROJ_SRCH = S.ACAD_LVL_PROJ_SRCH,
    T.ACAD_LEVEL_PROJ_CL = S.ACAD_LEVEL_PROJ_CL,
    T.ACAD_PLAN = S.ACAD_PLAN,
    T.ACAD_PLAN_SRCH = S.ACAD_PLAN_SRCH,
    T.ACAD_PLAN_INC = S.ACAD_PLAN_INC,
    T.ACADEMIC_LOAD = S.ACADEMIC_LOAD,
    T.TUITION_RES = S.TUITION_RES,
    T.PRT_BILL_FLAG = S.PRT_BILL_FLAG,
    T.RE_PRT_BILL_FLAG = S.RE_PRT_BILL_FLAG,
    T.PRT_DTTM_STAMP = S.PRT_DTTM_STAMP,
    T.RE_PRT_DTTM_STAMP = S.RE_PRT_DTTM_STAMP,
    T.INVOICE_DT = S.INVOICE_DT,
    T.PRIOR_IVC_BALANCE = S.PRIOR_IVC_BALANCE,
    T.MESSAGE_NBR = S.MESSAGE_NBR,
    T.SSF_ERROR_WARN = S.SSF_ERROR_WARN,
    T.CONTACT_NAME = S.CONTACT_NAME,
    T.TYPE_OF_REQUEST = S.TYPE_OF_REQUEST,
    T.BILL_BY_IND = S.BILL_BY_IND,
    T.BI_SCAN_LINE = S.BI_SCAN_LINE,
    T.CONTRACT_NUM = S.CONTRACT_NUM,
    T.CONTRACT_EMPLID = S.CONTRACT_EMPLID,
    T.CAMPUS = S.CAMPUS,
    T.SSF_LATEFEE_POST = S.SSF_LATEFEE_POST,
    T.SSF_CR_INV_IND = S.SSF_CR_INV_IND,
    T.SCC_ROW_ADD_OPRID = S.SCC_ROW_ADD_OPRID,
    T.SCC_ROW_ADD_DTTM = S.SCC_ROW_ADD_DTTM,
    T.SCC_ROW_UPD_OPRID = S.SCC_ROW_UPD_OPRID,
    T.SCC_ROW_UPD_DTTM = S.SCC_ROW_UPD_DTTM,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
    where 
    T.EMPLID <> S.EMPLID or 
    T.EXT_ORG_ID <> S.EXT_ORG_ID or 
    T.ADDRESS_TYPE <> S.ADDRESS_TYPE or 
    T.BILL_TYPE_ID <> S.BILL_TYPE_ID or 
    T.PRIOR_INVOICE_ID <> S.PRIOR_INVOICE_ID or 
    T.INVOICE_TYPE <> S.INVOICE_TYPE or 
    T.BILL_REQ_ID <> S.BILL_REQ_ID or 
    T.BI_REQ_NBR <> S.BI_REQ_NBR or 
    T.BILL_DATE_TIME <> S.BILL_DATE_TIME or 
    T.DUE_DT <> S.DUE_DT or 
    T.OPRID <> S.OPRID or 
    T.TOTAL_BILL <> S.TOTAL_BILL or 
    T.SF_BILL_STATUS <> S.SF_BILL_STATUS or 
    T.NAME <> S.NAME or 
    T.COUNTRY <> S.COUNTRY or 
    T.ADDRESS1 <> S.ADDRESS1 or 
    T.ADDRESS2 <> S.ADDRESS2 or 
    T.ADDRESS3 <> S.ADDRESS3 or 
    T.ADDRESS4 <> S.ADDRESS4 or 
    T.CITY <> S.CITY or 
    T.NUM1 <> S.NUM1 or 
    T.NUM2 <> S.NUM2 or 
    T.HOUSE_TYPE <> S.HOUSE_TYPE or 
    T.ADDR_FIELD1 <> S.ADDR_FIELD1 or 
    T.ADDR_FIELD2 <> S.ADDR_FIELD2 or 
    T.ADDR_FIELD3 <> S.ADDR_FIELD3 or 
    T.COUNTY <> S.COUNTY or 
    T.STATE <> S.STATE or 
    T.POSTAL <> S.POSTAL or 
    T.GEO_CODE <> S.GEO_CODE or 
    T.IN_CITY_LIMIT <> S.IN_CITY_LIMIT or 
    T.ACAD_CAREER <> S.ACAD_CAREER or 
    T.ACAD_CAREER_SRCH <> S.ACAD_CAREER_SRCH or 
    T.ACAD_CAREER_INC <> S.ACAD_CAREER_INC or 
    T.ACAD_PROG_PRIMARY <> S.ACAD_PROG_PRIMARY or 
    T.ACAD_PROG_SRCH <> S.ACAD_PROG_SRCH or 
    T.ACAD_PROG <> S.ACAD_PROG or 
    T.ACAD_LEVEL_PROJ <> S.ACAD_LEVEL_PROJ or 
    T.ACAD_LVL_PROJ_SRCH <> S.ACAD_LVL_PROJ_SRCH or 
    T.ACAD_LEVEL_PROJ_CL <> S.ACAD_LEVEL_PROJ_CL or 
    T.ACAD_PLAN <> S.ACAD_PLAN or 
    T.ACAD_PLAN_SRCH <> S.ACAD_PLAN_SRCH or 
    T.ACAD_PLAN_INC <> S.ACAD_PLAN_INC or 
    T.ACADEMIC_LOAD <> S.ACADEMIC_LOAD or 
    T.TUITION_RES <> S.TUITION_RES or 
    T.PRT_BILL_FLAG <> S.PRT_BILL_FLAG or 
    T.RE_PRT_BILL_FLAG <> S.RE_PRT_BILL_FLAG or 
    nvl(T.PRT_DTTM_STAMP,to_date('01-JAN-1900')) <> nvl(S.PRT_DTTM_STAMP,to_date('01-JAN-1900')) or 
    nvl(T.RE_PRT_DTTM_STAMP,to_date('01-JAN-1900')) <> nvl(S.RE_PRT_DTTM_STAMP,to_date('01-JAN-1900')) or 
    nvl(T.INVOICE_DT,to_date('01-JAN-1900')) <> nvl(S.INVOICE_DT,to_date('01-JAN-1900')) or 
    T.PRIOR_IVC_BALANCE <> S.PRIOR_IVC_BALANCE or 
    T.MESSAGE_NBR <> S.MESSAGE_NBR or 
    T.SSF_ERROR_WARN <> S.SSF_ERROR_WARN or 
    T.CONTACT_NAME <> S.CONTACT_NAME or 
    T.TYPE_OF_REQUEST <> S.TYPE_OF_REQUEST or 
    T.BILL_BY_IND <> S.BILL_BY_IND or 
    T.BI_SCAN_LINE <> S.BI_SCAN_LINE or 
    T.CONTRACT_NUM <> S.CONTRACT_NUM or 
    T.CONTRACT_EMPLID <> S.CONTRACT_EMPLID or 
    T.CAMPUS <> S.CAMPUS or 
    T.SSF_LATEFEE_POST <> S.SSF_LATEFEE_POST or 
    T.SSF_CR_INV_IND <> S.SSF_CR_INV_IND or 
    T.SCC_ROW_ADD_OPRID <> S.SCC_ROW_ADD_OPRID or 
    nvl(T.SCC_ROW_ADD_DTTM,to_date('01-JAN-1900')) <> nvl(S.SCC_ROW_ADD_DTTM,to_date('01-JAN-1900')) or 
    T.SCC_ROW_UPD_OPRID <> S.SCC_ROW_UPD_OPRID or 
    nvl(T.SCC_ROW_UPD_DTTM,to_date('01-JAN-1900')) <> nvl(S.SCC_ROW_UPD_DTTM,to_date('01-JAN-1900')) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.BUSINESS_UNIT,
    T.INVOICE_ID, 
    T.SRC_SYS_ID, 
    T.EMPLID, 
    T.EXT_ORG_ID, 
    T.ADDRESS_TYPE, 
    T.BILL_TYPE_ID, 
    T.PRIOR_INVOICE_ID, 
    T.INVOICE_TYPE, 
    T.BILL_REQ_ID,
    T.BI_REQ_NBR, 
    T.BILL_DATE_TIME, 
    T.DUE_DT, 
    T.OPRID,
    T.TOTAL_BILL, 
    T.SF_BILL_STATUS, 
    T.NAME, 
    T.COUNTRY,
    T.ADDRESS1, 
    T.ADDRESS2, 
    T.ADDRESS3, 
    T.ADDRESS4, 
    T.CITY, 
    T.NUM1, 
    T.NUM2, 
    T.HOUSE_TYPE, 
    T.ADDR_FIELD1,
    T.ADDR_FIELD2,
    T.ADDR_FIELD3,
    T.COUNTY, 
    T.STATE,
    T.POSTAL, 
    T.GEO_CODE, 
    T.IN_CITY_LIMIT,
    T.ACAD_CAREER,
    T.ACAD_CAREER_SRCH, 
    T.ACAD_CAREER_INC,
    T.ACAD_PROG_PRIMARY,
    T.ACAD_PROG_SRCH, 
    T.ACAD_PROG,
    T.ACAD_LEVEL_PROJ,
    T.ACAD_LVL_PROJ_SRCH, 
    T.ACAD_LEVEL_PROJ_CL, 
    T.ACAD_PLAN,
    T.ACAD_PLAN_SRCH, 
    T.ACAD_PLAN_INC,
    T.ACADEMIC_LOAD,
    T.TUITION_RES,
    T.PRT_BILL_FLAG,
    T.RE_PRT_BILL_FLAG, 
    T.PRT_DTTM_STAMP, 
    T.RE_PRT_DTTM_STAMP,
    T.INVOICE_DT, 
    T.PRIOR_IVC_BALANCE,
    T.MESSAGE_NBR,
    T.SSF_ERROR_WARN, 
    T.CONTACT_NAME, 
    T.TYPE_OF_REQUEST,
    T.BILL_BY_IND,
    T.BI_SCAN_LINE, 
    T.CONTRACT_NUM, 
    T.CONTRACT_EMPLID,
    T.CAMPUS, 
    T.SSF_LATEFEE_POST, 
    T.SSF_CR_INV_IND, 
    T.SCC_ROW_ADD_OPRID,
    T.SCC_ROW_ADD_DTTM, 
    T.SCC_ROW_UPD_OPRID,
    T.SCC_ROW_UPD_DTTM, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.BUSINESS_UNIT,
    S.INVOICE_ID, 
    'CS90', 
    S.EMPLID, 
    S.EXT_ORG_ID, 
    S.ADDRESS_TYPE, 
    S.BILL_TYPE_ID, 
    S.PRIOR_INVOICE_ID, 
    S.INVOICE_TYPE, 
    S.BILL_REQ_ID,
    S.BI_REQ_NBR, 
    S.BILL_DATE_TIME, 
    S.DUE_DT, 
    S.OPRID,
    S.TOTAL_BILL, 
    S.SF_BILL_STATUS, 
    S.NAME, 
    S.COUNTRY,
    S.ADDRESS1, 
    S.ADDRESS2, 
    S.ADDRESS3, 
    S.ADDRESS4, 
    S.CITY, 
    S.NUM1, 
    S.NUM2, 
    S.HOUSE_TYPE, 
    S.ADDR_FIELD1,
    S.ADDR_FIELD2,
    S.ADDR_FIELD3,
    S.COUNTY, 
    S.STATE,
    S.POSTAL, 
    S.GEO_CODE, 
    S.IN_CITY_LIMIT,
    S.ACAD_CAREER,
    S.ACAD_CAREER_SRCH, 
    S.ACAD_CAREER_INC,
    S.ACAD_PROG_PRIMARY,
    S.ACAD_PROG_SRCH, 
    S.ACAD_PROG,
    S.ACAD_LEVEL_PROJ,
    S.ACAD_LVL_PROJ_SRCH, 
    S.ACAD_LEVEL_PROJ_CL, 
    S.ACAD_PLAN,
    S.ACAD_PLAN_SRCH, 
    S.ACAD_PLAN_INC,
    S.ACADEMIC_LOAD,
    S.TUITION_RES,
    S.PRT_BILL_FLAG,
    S.RE_PRT_BILL_FLAG, 
    S.PRT_DTTM_STAMP, 
    S.RE_PRT_DTTM_STAMP,
    S.INVOICE_DT, 
    S.PRIOR_IVC_BALANCE,
    S.MESSAGE_NBR,
    S.SSF_ERROR_WARN, 
    S.CONTACT_NAME, 
    S.TYPE_OF_REQUEST,
    S.BILL_BY_IND,
    S.BI_SCAN_LINE, 
    S.CONTRACT_NUM, 
    S.CONTRACT_EMPLID,
    S.CAMPUS, 
    S.SSF_LATEFEE_POST, 
    S.SSF_CR_INV_IND, 
    S.SCC_ROW_ADD_OPRID,
    S.SCC_ROW_ADD_DTTM, 
    S.SCC_ROW_UPD_OPRID,
    S.SCC_ROW_UPD_DTTM, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_BI_BILL_HEADER rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_BI_BILL_HEADER',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_BI_BILL_HEADER';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_BI_BILL_HEADER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_BI_BILL_HEADER';
update AMSTG_OWNER.PS_BI_BILL_HEADER T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select BUSINESS_UNIT, INVOICE_ID
   from AMSTG_OWNER.PS_BI_BILL_HEADER T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_BI_BILL_HEADER') = 'Y'
  minus
 select BUSINESS_UNIT, INVOICE_ID
   from SYSADM.PS_BI_BILL_HEADER@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_BI_BILL_HEADER') = 'Y'
   ) S
 where T.BUSINESS_UNIT = S.BUSINESS_UNIT
   and T.INVOICE_ID = S.INVOICE_ID
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_BI_BILL_HEADER rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_BI_BILL_HEADER',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_BI_BILL_HEADER'
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

END AM_PS_BI_BILL_HEADER_P;
/
