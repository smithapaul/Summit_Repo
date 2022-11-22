DROP PROCEDURE CSMRT_OWNER.PS_ADM_APPL_TENDER_P
/

--
-- PS_ADM_APPL_TENDER_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_ADM_APPL_TENDER_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ADM_APPL_TENDER'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_ADM_APPL_TENDER', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ADM_APPL_TENDER'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ADM_APPL_TENDER from PeopleSoft table PS_ADM_APPL_TENDER.
--
 --V01  SMT-xxxx 10/02/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ADM_APPL_TENDER';
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
 where TABLE_NAME = 'PS_ADM_APPL_TENDER'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ADM_APPL_TENDER@SASOURCE S)
 where TABLE_NAME = 'PS_ADM_APPL_TENDER'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ADM_APPL_TENDER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ADM_APPL_TENDER';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ADM_APPL_TENDER T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(trim(TENDER_CATEGORY),'-') TENDER_CATEGORY, 
    nvl(TENDER_AMT,0) TENDER_AMT, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(ORIGNL_TENDER_AMT,0) ORIGNL_TENDER_AMT, 
    nvl(trim(ORIGNL_CURRENCY_CD),'-') ORIGNL_CURRENCY_CD, 
    nvl(trim(CUR_RT_TYPE),'-') CUR_RT_TYPE, 
    nvl(RATE_MULT,0) RATE_MULT, 
    nvl(RATE_DIV,0) RATE_DIV, 
    nvl(trim(CHECK_NUM_SF),'-') CHECK_NUM_SF, 
    nvl(trim(FED_RSRV_BANK_ID),'-') FED_RSRV_BANK_ID, 
    nvl(trim(BANK_ACCT_TYP),'-') BANK_ACCT_TYP, 
    nvl(trim(BANK_ACCT_NAME),'-') BANK_ACCT_NAME, 
    nvl(trim(SSF_BNK_ACCT_NUM),'-') SSF_BNK_ACCT_NUM, 
    nvl(trim(THIRD_PARTY),'-') THIRD_PARTY, 
    nvl(trim(REF1_DESCR),'-') REF1_DESCR, 
    nvl(trim(TRACER_NBR),'-') TRACER_NBR, 
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
    nvl(trim(EMAIL_ADDR),'-') EMAIL_ADDR, 
    nvl(trim(PHONE),'-') PHONE, 
    nvl(trim(CR_CARD_NBR),'-') CR_CARD_NBR, 
    nvl(trim(CR_CARD_TYPE),'-') CR_CARD_TYPE, 
    nvl(trim(CR_CARD_FNAME),'-') CR_CARD_FNAME, 
    nvl(trim(CR_CARD_LNAME),'-') CR_CARD_LNAME, 
    --to_date(to_char(case when CR_CARD_EXP_DT < '01-JAN-1800' then NULL else CR_CARD_EXP_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CR_CARD_EXP_DT,
    CR_CARD_EXP_DT,
    nvl(trim(CR_CARD_STATUS),'-') CR_CARD_STATUS, 
    nvl(trim(CR_CARD_ERRMSG),'-') CR_CARD_ERRMSG, 
    nvl(trim(CR_CARD_VDAUTH),'-') CR_CARD_VDAUTH, 
    nvl(trim(CR_CARD_DECLND),'-') CR_CARD_DECLND, 
    nvl(trim(CR_CARD_ISSUER),'-') CR_CARD_ISSUER, 
    nvl(trim(CR_CARD_AUTH_CD),'-') CR_CARD_AUTH_CD, 
    CR_CARD_AUTH_DT, 
    nvl(trim(CR_CARD_AUTH_REPLY),'-') CR_CARD_AUTH_REPLY, 
    nvl(trim(CR_CARD_AVS_CD),'-') CR_CARD_AVS_CD, 
    nvl(trim(CR_CARD_RQST_ID),'-') CR_CARD_RQST_ID, 
    nvl(trim(CR_CARD_A_DTTM),'-') CR_CARD_A_DTTM, 
    nvl(trim(SF_MERCHANT_ID),'-') SF_MERCHANT_ID, 
    nvl(trim(CR_CARD_SRVC_PROV),'-') CR_CARD_SRVC_PROV, 
    nvl(trim(SF_PMT_REF_NBR),'-') SF_PMT_REF_NBR, 
    nvl(trim(CR_CARD_DIGITS),'-') CR_CARD_DIGITS, 
    nvl(trim(BANK_CD),'-') BANK_CD
from SYSADM.PS_ADM_APPL_TENDER@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_TENDER')
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8  ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and 
    T.ADM_APPL_NBR = S.ADM_APPL_NBR and 
    T.TENDER_CATEGORY = S.TENDER_CATEGORY and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.TENDER_AMT = S.TENDER_AMT,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.ORIGNL_TENDER_AMT = S.ORIGNL_TENDER_AMT,
    T.ORIGNL_CURRENCY_CD = S.ORIGNL_CURRENCY_CD,
    T.CUR_RT_TYPE = S.CUR_RT_TYPE,
    T.RATE_MULT = S.RATE_MULT,
    T.RATE_DIV = S.RATE_DIV,
    T.CHECK_NUM_SF = S.CHECK_NUM_SF,
    T.FED_RSRV_BANK_ID = S.FED_RSRV_BANK_ID,
    T.BANK_ACCT_TYP = S.BANK_ACCT_TYP,
    T.BANK_ACCT_NAME = S.BANK_ACCT_NAME,
    T.SSF_BNK_ACCT_NUM = S.SSF_BNK_ACCT_NUM,
    T.THIRD_PARTY = S.THIRD_PARTY,
    T.REF1_DESCR = S.REF1_DESCR,
    T.TRACER_NBR = S.TRACER_NBR,
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
    T.EMAIL_ADDR = S.EMAIL_ADDR,
    T.PHONE = S.PHONE,
    T.CR_CARD_NBR = S.CR_CARD_NBR,
    T.CR_CARD_TYPE = S.CR_CARD_TYPE,
    T.CR_CARD_FNAME = S.CR_CARD_FNAME,
    T.CR_CARD_LNAME = S.CR_CARD_LNAME,
    T.CR_CARD_EXP_DT = S.CR_CARD_EXP_DT,
    T.CR_CARD_STATUS = S.CR_CARD_STATUS,
    T.CR_CARD_ERRMSG = S.CR_CARD_ERRMSG,
    T.CR_CARD_VDAUTH = S.CR_CARD_VDAUTH,
    T.CR_CARD_DECLND = S.CR_CARD_DECLND,
    T.CR_CARD_ISSUER = S.CR_CARD_ISSUER,
    T.CR_CARD_AUTH_CD = S.CR_CARD_AUTH_CD,
    T.CR_CARD_AUTH_DT = S.CR_CARD_AUTH_DT,
    T.CR_CARD_AUTH_REPLY = S.CR_CARD_AUTH_REPLY,
    T.CR_CARD_AVS_CD = S.CR_CARD_AVS_CD,
    T.CR_CARD_RQST_ID = S.CR_CARD_RQST_ID,
    T.CR_CARD_A_DTTM = S.CR_CARD_A_DTTM,
    T.SF_MERCHANT_ID = S.SF_MERCHANT_ID,
    T.CR_CARD_SRVC_PROV = S.CR_CARD_SRVC_PROV,
    T.SF_PMT_REF_NBR = S.SF_PMT_REF_NBR,
    T.CR_CARD_DIGITS = S.CR_CARD_DIGITS,
    T.BANK_CD = S.BANK_CD,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.TENDER_AMT <> S.TENDER_AMT or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.ORIGNL_TENDER_AMT <> S.ORIGNL_TENDER_AMT or 
    T.ORIGNL_CURRENCY_CD <> S.ORIGNL_CURRENCY_CD or 
    T.CUR_RT_TYPE <> S.CUR_RT_TYPE or 
    T.RATE_MULT <> S.RATE_MULT or 
    T.RATE_DIV <> S.RATE_DIV or 
    T.CHECK_NUM_SF <> S.CHECK_NUM_SF or 
    T.FED_RSRV_BANK_ID <> S.FED_RSRV_BANK_ID or 
    T.BANK_ACCT_TYP <> S.BANK_ACCT_TYP or 
    T.BANK_ACCT_NAME <> S.BANK_ACCT_NAME or 
    T.SSF_BNK_ACCT_NUM <> S.SSF_BNK_ACCT_NUM or 
    T.THIRD_PARTY <> S.THIRD_PARTY or 
    T.REF1_DESCR <> S.REF1_DESCR or 
    T.TRACER_NBR <> S.TRACER_NBR or 
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
    T.EMAIL_ADDR <> S.EMAIL_ADDR or 
    T.PHONE <> S.PHONE or 
    T.CR_CARD_NBR <> S.CR_CARD_NBR or 
    T.CR_CARD_TYPE <> S.CR_CARD_TYPE or 
    T.CR_CARD_FNAME <> S.CR_CARD_FNAME or 
    T.CR_CARD_LNAME <> S.CR_CARD_LNAME or 
    nvl(trim(T.CR_CARD_EXP_DT),0) <> nvl(trim(S.CR_CARD_EXP_DT),0) or 
    T.CR_CARD_STATUS <> S.CR_CARD_STATUS or 
    T.CR_CARD_ERRMSG <> S.CR_CARD_ERRMSG or 
    T.CR_CARD_VDAUTH <> S.CR_CARD_VDAUTH or 
    T.CR_CARD_DECLND <> S.CR_CARD_DECLND or 
    T.CR_CARD_ISSUER <> S.CR_CARD_ISSUER or 
    T.CR_CARD_AUTH_CD <> S.CR_CARD_AUTH_CD or 
    nvl(trim(T.CR_CARD_AUTH_DT),0) <> nvl(trim(S.CR_CARD_AUTH_DT),0) or 
    T.CR_CARD_AUTH_REPLY <> S.CR_CARD_AUTH_REPLY or 
    T.CR_CARD_AVS_CD <> S.CR_CARD_AVS_CD or 
    T.CR_CARD_RQST_ID <> S.CR_CARD_RQST_ID or 
    T.CR_CARD_A_DTTM <> S.CR_CARD_A_DTTM or 
    T.SF_MERCHANT_ID <> S.SF_MERCHANT_ID or 
    T.CR_CARD_SRVC_PROV <> S.CR_CARD_SRVC_PROV or 
    T.SF_PMT_REF_NBR <> S.SF_PMT_REF_NBR or 
    T.CR_CARD_DIGITS <> S.CR_CARD_DIGITS or 
    T.BANK_CD <> S.BANK_CD or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.STDNT_CAR_NBR,
    T.ADM_APPL_NBR, 
    T.TENDER_CATEGORY,
    T.SRC_SYS_ID, 
    T.TENDER_AMT, 
    T.CURRENCY_CD,
    T.ORIGNL_TENDER_AMT,
    T.ORIGNL_CURRENCY_CD, 
    T.CUR_RT_TYPE,
    T.RATE_MULT,
    T.RATE_DIV, 
    T.CHECK_NUM_SF, 
    T.FED_RSRV_BANK_ID, 
    T.BANK_ACCT_TYP,
    T.BANK_ACCT_NAME, 
    T.SSF_BNK_ACCT_NUM, 
    T.THIRD_PARTY,
    T.REF1_DESCR, 
    T.TRACER_NBR, 
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
    T.EMAIL_ADDR, 
    T.PHONE,
    T.CR_CARD_NBR,
    T.CR_CARD_TYPE, 
    T.CR_CARD_FNAME,
    T.CR_CARD_LNAME,
    T.CR_CARD_EXP_DT, 
    T.CR_CARD_STATUS, 
    T.CR_CARD_ERRMSG, 
    T.CR_CARD_VDAUTH, 
    T.CR_CARD_DECLND, 
    T.CR_CARD_ISSUER, 
    T.CR_CARD_AUTH_CD,
    T.CR_CARD_AUTH_DT,
    T.CR_CARD_AUTH_REPLY, 
    T.CR_CARD_AVS_CD, 
    T.CR_CARD_RQST_ID,
    T.CR_CARD_A_DTTM, 
    T.SF_MERCHANT_ID, 
    T.CR_CARD_SRVC_PROV,
    T.SF_PMT_REF_NBR, 
    T.CR_CARD_DIGITS, 
    T.BANK_CD,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.STDNT_CAR_NBR,
    S.ADM_APPL_NBR, 
    S.TENDER_CATEGORY,
    'CS90', 
    S.TENDER_AMT, 
    S.CURRENCY_CD,
    S.ORIGNL_TENDER_AMT,
    S.ORIGNL_CURRENCY_CD, 
    S.CUR_RT_TYPE,
    S.RATE_MULT,
    S.RATE_DIV, 
    S.CHECK_NUM_SF, 
    S.FED_RSRV_BANK_ID, 
    S.BANK_ACCT_TYP,
    S.BANK_ACCT_NAME, 
    S.SSF_BNK_ACCT_NUM, 
    S.THIRD_PARTY,
    S.REF1_DESCR, 
    S.TRACER_NBR, 
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
    S.EMAIL_ADDR, 
    S.PHONE,
    S.CR_CARD_NBR,
    S.CR_CARD_TYPE, 
    S.CR_CARD_FNAME,
    S.CR_CARD_LNAME,
    S.CR_CARD_EXP_DT, 
    S.CR_CARD_STATUS, 
    S.CR_CARD_ERRMSG, 
    S.CR_CARD_VDAUTH, 
    S.CR_CARD_DECLND, 
    S.CR_CARD_ISSUER, 
    S.CR_CARD_AUTH_CD,
    S.CR_CARD_AUTH_DT,
    S.CR_CARD_AUTH_REPLY, 
    S.CR_CARD_AVS_CD, 
    S.CR_CARD_RQST_ID,
    S.CR_CARD_A_DTTM, 
    S.SF_MERCHANT_ID, 
    S.CR_CARD_SRVC_PROV,
    S.SF_PMT_REF_NBR, 
    S.CR_CARD_DIGITS, 
    S.BANK_CD,
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPL_TENDER rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPL_TENDER',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ADM_APPL_TENDER';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPL_TENDER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ADM_APPL_TENDER';
update CSSTG_OWNER.PS_ADM_APPL_TENDER T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, TENDER_CATEGORY
   from CSSTG_OWNER.PS_ADM_APPL_TENDER T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_TENDER') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, TENDER_CATEGORY
   from SYSADM.PS_ADM_APPL_TENDER@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ADM_APPL_TENDER') = 'Y' 
   ) S
 where T.EMPLID= S.EMPLID
    AND T.ACAD_CAREER = S.ACAD_CAREER
    AND T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
    AND T.ADM_APPL_NBR = S.ADM_APPL_NBR
    AND T.TENDER_CATEGORY = S.TENDER_CATEGORY
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ADM_APPL_TENDER rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ADM_APPL_TENDER',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ADM_APPL_TENDER'
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

END PS_ADM_APPL_TENDER_P;
/
