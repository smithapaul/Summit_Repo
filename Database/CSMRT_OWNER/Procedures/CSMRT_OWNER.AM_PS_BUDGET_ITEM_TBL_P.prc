DROP PROCEDURE CSMRT_OWNER.AM_PS_BUDGET_ITEM_TBL_P
/

--
-- AM_PS_BUDGET_ITEM_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_BUDGET_ITEM_TBL_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_BUDGET_ITEM_TBL from PeopleSoft table PS_BUDGET_ITEM_TBL.
--
-- V01  SMT-xxxx 8/04/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_BUDGET_ITEM_TBL';
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
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_BUDGET_ITEM_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_BUDGET_ITEM_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_BUDGET_ITEM_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_BUDGET_ITEM_TBL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_BUDGET_ITEM_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into AMSTG_OWNER.PS_T_BUDGET_ITEM_TBL
select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    nvl(trim(BGT_ITEM_CATEGORY),'-') BGT_ITEM_CATEGORY, 
    nvl(trim(BUDGET_ITEM_CD),'-') BUDGET_ITEM_CD, 
    'CS90' SRC_SYS_ID,
    DESCR, 
    DESCRSHORT, 
    BUDGET_ITEM_AMOUNT, 
    PELL_ITEM_AMOUNT, 
    BUDG_MULTIPLIER, 
    BUDG_MULT_FIELD, 
    CURRENCY_CD, 
    THREE_QTR_TIME_ADJ, 
    HALF_TIME_ADJ, 
    LESS_HALF_TIME_ADJ, 
    ZERO_TIME_ADJ, 
    SFA_PELITMAMT_LHT,  
    to_char(substr(trim(DESCRLONG), 1, 4000)) DESCRLONG,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_BUDGET_ITEM_TBL@AMSOURCE S
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_BUDGET_ITEM_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_BUDGET_ITEM_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_BUDGET_ITEM_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_BUDGET_ITEM_TBL T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    nvl(trim(BGT_ITEM_CATEGORY),'-') BGT_ITEM_CATEGORY, 
    nvl(trim(BUDGET_ITEM_CD),'-') BUDGET_ITEM_CD, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(BUDGET_ITEM_AMOUNT,0) BUDGET_ITEM_AMOUNT, 
    nvl(PELL_ITEM_AMOUNT,0) PELL_ITEM_AMOUNT, 
    nvl(trim(BUDG_MULTIPLIER),'-') BUDG_MULTIPLIER, 
    nvl(trim(BUDG_MULT_FIELD),'-') BUDG_MULT_FIELD, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(THREE_QTR_TIME_ADJ,0) THREE_QTR_TIME_ADJ, 
    nvl(HALF_TIME_ADJ,0) HALF_TIME_ADJ, 
    nvl(LESS_HALF_TIME_ADJ,0) LESS_HALF_TIME_ADJ, 
    nvl(ZERO_TIME_ADJ,0) ZERO_TIME_ADJ, 
    nvl(SFA_PELITMAMT_LHT,0) SFA_PELITMAMT_LHT, 
    DESCRLONG DESCRLONG
from AMSTG_OWNER.PS_T_BUDGET_ITEM_TBL S 
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_BUDGET_ITEM_TBL') ) S
 on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.AID_YEAR = S.AID_YEAR and 
    T.BGT_ITEM_CATEGORY = S.BGT_ITEM_CATEGORY and 
    T.BUDGET_ITEM_CD = S.BUDGET_ITEM_CD and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.BUDGET_ITEM_AMOUNT = S.BUDGET_ITEM_AMOUNT,
    T.PELL_ITEM_AMOUNT = S.PELL_ITEM_AMOUNT,
    T.BUDG_MULTIPLIER = S.BUDG_MULTIPLIER,
    T.BUDG_MULT_FIELD = S.BUDG_MULT_FIELD,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.THREE_QTR_TIME_ADJ = S.THREE_QTR_TIME_ADJ,
    T.HALF_TIME_ADJ = S.HALF_TIME_ADJ,
    T.LESS_HALF_TIME_ADJ = S.LESS_HALF_TIME_ADJ,
    T.ZERO_TIME_ADJ = S.ZERO_TIME_ADJ,
    T.SFA_PELITMAMT_LHT = S.SFA_PELITMAMT_LHT,
    T.DESCRLONG = S.DESCRLONG,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.BUDGET_ITEM_AMOUNT <> S.BUDGET_ITEM_AMOUNT or 
    T.PELL_ITEM_AMOUNT <> S.PELL_ITEM_AMOUNT or 
    T.BUDG_MULTIPLIER <> S.BUDG_MULTIPLIER or 
    T.BUDG_MULT_FIELD <> S.BUDG_MULT_FIELD or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.THREE_QTR_TIME_ADJ <> S.THREE_QTR_TIME_ADJ or 
    T.HALF_TIME_ADJ <> S.HALF_TIME_ADJ or 
    T.LESS_HALF_TIME_ADJ <> S.LESS_HALF_TIME_ADJ or 
    T.ZERO_TIME_ADJ <> S.ZERO_TIME_ADJ or 
    T.SFA_PELITMAMT_LHT <> S.SFA_PELITMAMT_LHT or 
    nvl(trim(T.DESCRLONG),0) <> nvl(trim(S.DESCRLONG),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.AID_YEAR, 
    T.BGT_ITEM_CATEGORY,
    T.BUDGET_ITEM_CD, 
    T.SRC_SYS_ID, 
    T.DESCR,
    T.DESCRSHORT, 
    T.BUDGET_ITEM_AMOUNT, 
    T.PELL_ITEM_AMOUNT, 
    T.BUDG_MULTIPLIER,
    T.BUDG_MULT_FIELD,
    T.CURRENCY_CD,
    T.THREE_QTR_TIME_ADJ, 
    T.HALF_TIME_ADJ,
    T.LESS_HALF_TIME_ADJ, 
    T.ZERO_TIME_ADJ,
    T.SFA_PELITMAMT_LHT,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.DESCRLONG
    ) 
values (
    S.INSTITUTION,
    S.AID_YEAR, 
    S.BGT_ITEM_CATEGORY,
    S.BUDGET_ITEM_CD, 
    'CS90', 
    S.DESCR,
    S.DESCRSHORT, 
    S.BUDGET_ITEM_AMOUNT, 
    S.PELL_ITEM_AMOUNT, 
    S.BUDG_MULTIPLIER,
    S.BUDG_MULT_FIELD,
    S.CURRENCY_CD,
    S.THREE_QTR_TIME_ADJ, 
    S.HALF_TIME_ADJ,
    S.LESS_HALF_TIME_ADJ, 
    S.ZERO_TIME_ADJ,
    S.SFA_PELITMAMT_LHT,
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.DESCRLONG)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_BUDGET_ITEM_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_BUDGET_ITEM_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_BUDGET_ITEM_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_BUDGET_ITEM_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_BUDGET_ITEM_TBL';
update AMSTG_OWNER.PS_BUDGET_ITEM_TBL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, AID_YEAR, BGT_ITEM_CATEGORY, BUDGET_ITEM_CD
   from AMSTG_OWNER.PS_BUDGET_ITEM_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_BUDGET_ITEM_TBL') = 'Y'
  minus
 select INSTITUTION, AID_YEAR, BGT_ITEM_CATEGORY, BUDGET_ITEM_CD
   from SYSADM.PS_BUDGET_ITEM_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_BUDGET_ITEM_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.BGT_ITEM_CATEGORY = S.BGT_ITEM_CATEGORY
   and T.BUDGET_ITEM_CD = S.BUDGET_ITEM_CD
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_BUDGET_ITEM_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_BUDGET_ITEM_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_BUDGET_ITEM_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);

END AM_PS_BUDGET_ITEM_TBL_P;
/
