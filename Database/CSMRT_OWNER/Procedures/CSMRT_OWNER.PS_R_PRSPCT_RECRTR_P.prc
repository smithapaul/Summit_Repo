DROP PROCEDURE CSMRT_OWNER.PS_R_PRSPCT_RECRTR_P
/

--
-- PS_R_PRSPCT_RECRTR_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_R_PRSPCT_RECRTR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table PS_R_PRSPCT_RECRTR.
--
 --V01  SMT-xxxx 02/11/2019,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_R_PRSPCT_RECRTR';
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

strMessage01    := 'Truncating table CSMRT_OWNER.PS_R_PRSPCT_RECRTR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_R_PRSPCT_RECRTR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.PS_R_PRSPCT_RECRTR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_R_PRSPCT_RECRTR';
insert into CSMRT_OWNER.PS_R_PRSPCT_RECRTR           -- Temp for insert!!!!!!!!!!!!!!
with 
CAR as (
select /*+ parallel(8) inline */
       distinct 
       INSTITUTION_CD, 
       ACAD_CAR_CD, 
       EMPLID PERSON_ID,
       SRC_SYS_ID
  from UM_D_PRSPCT_CAR),
REC as (
SELECT /*+ parallel(8) inline */ 
       CAR.INSTITUTION_CD,
       CAR.ACAD_CAR_CD,
       CAR.PERSON_ID,
       nvl(B.RECRUITER_ID,'-') RECRTR_ID,
       nvl(A.RECRUITMENT_CAT,'-') RECRT_CTGRY_ID,
       CAR.SRC_SYS_ID,
       nvl(B.PRIMARY_FLAG,'-') PRI_RCRTR_FLG,
       nvl(A.RECRUIT_SUB_CAT,'-') RECRT_SUB_CTGRY_ID 
  FROM CAR 
  LEFT OUTER JOIN CSSTG_OWNER.PS_PRSPCT_RCR_CAT A
    on CAR.INSTITUTION_CD = A.INSTITUTION 
   and CAR.ACAD_CAR_CD = A.ACAD_CAREER
   and CAR.PERSON_ID = A.EMPLID
   and CAR.SRC_SYS_ID = A.SRC_SYS_ID
   and A.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSSTG_OWNER.PS_PRSPCT_RECRTER B 
    on CAR.INSTITUTION_CD = B.INSTITUTION 
   and CAR.ACAD_CAR_CD = B.ACAD_CAREER 
   and CAR.PERSON_ID = B.EMPLID 
   and nvl(A.RECRUITMENT_CAT,'-') = B.RECRUITMENT_CAT 
   and CAR.SRC_SYS_ID = B.SRC_SYS_ID
   and B.DATA_ORIGIN <> 'D')
select /*+ parallel(8) */ 
       REC.INSTITUTION_CD,
       REC.ACAD_CAR_CD,
       REC.PERSON_ID,
       REC.RECRTR_ID,
       REC.RECRT_CTGRY_ID,
       REC.SRC_SYS_ID,
       I.INSTITUTION_SID,
       nvl(C1.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(P1.PERSON_SID,2147483646) PRSPCT_SID,
--       nvl(P2.PERSON_SID,2147483646) RECRTR_SID,
       nvl(R.RECRTR_SID,2147483646) RECRTR_SID,
       nvl(C2.RECRT_CTGRY_SID,2147483646) RECRT_CTGRY_SID,
       REC.PRI_RCRTR_FLG,
       REC.RECRT_SUB_CTGRY_ID,
       nvl((select min(X.XLATSHORTNAME) 
              from UM_D_XLATITEM_VW X
             where X.FIELDNAME = 'RECRUIT_SUB_CAT'
               and X.FIELDVALUE = REC.RECRT_SUB_CTGRY_ID),'-') RECRT_SUB_CTGRY_SD, 
       nvl((select min(X.XLATLONGNAME) 
              from UM_D_XLATITEM_VW X
             where X.FIELDNAME = 'RECRUIT_SUB_CAT'
               and X.FIELDVALUE = REC.RECRT_SUB_CTGRY_ID),'-') RECRT_SUB_CTGRY_LD,
       ROW_NUMBER () OVER (PARTITION BY REC.INSTITUTION_CD, REC.ACAD_CAR_CD, REC.PERSON_ID, REC.SRC_SYS_ID
                               ORDER BY DECODE(REC.PRI_RCRTR_FLG, 'Y', 0, 9), REC.RECRTR_ID, REC.RECRT_CTGRY_ID) RECRTR_ORDER,
       ROW_NUMBER () OVER (PARTITION BY REC.INSTITUTION_CD, REC.ACAD_CAR_CD, REC.PERSON_ID, REC.SRC_SYS_ID
                               ORDER BY REC.RECRT_CTGRY_ID, REC.RECRT_SUB_CTGRY_ID DESC, DECODE(REC.PRI_RCRTR_FLG, 'Y', 0, 9), REC.RECRTR_ID) CTGRY_ORDER,
       'N','S', SYSDATE, SYSDATE, 1234 
from REC
join PS_D_INSTITUTION I
  on REC.INSTITUTION_CD = I.INSTITUTION_CD
 and REC.SRC_SYS_ID = I.SRC_SYS_ID
left outer join PS_D_ACAD_CAR C1
  on REC.INSTITUTION_CD = C1.INSTITUTION_CD
 and REC.ACAD_CAR_CD = C1.ACAD_CAR_CD
 and REC.SRC_SYS_ID = C1.SRC_SYS_ID
left outer join PS_D_PERSON P1
  on REC.PERSON_ID = P1.PERSON_ID
 and REC.SRC_SYS_ID = P1.SRC_SYS_ID
left outer join PS_D_RECRTR R
  on REC.RECRTR_ID = R.RECRUITER_ID
 and REC.INSTITUTION_CD = R.INSTITUTION_CD
 and REC.ACAD_CAR_CD = R.ACAD_CAR_CD
 and REC.SRC_SYS_ID = R.SRC_SYS_ID
left outer join PS_D_RECRT_CTGRY C2
  on REC.INSTITUTION_CD = C2.INSTITUTION_CD
 and REC.RECRT_CTGRY_ID = C2.RECRT_CTGRY_ID
 and REC.SRC_SYS_ID = C2.SRC_SYS_ID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_R_PRSPCT_RECRTR rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_R_PRSPCT_RECRTR',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_R_PRSPCT_RECRTR',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

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

END PS_R_PRSPCT_RECRTR_P;
/
