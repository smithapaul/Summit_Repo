DROP PROCEDURE CSMRT_OWNER.UM_F_FA_STDNT_RTN_TIV_P
/

--
-- UM_F_FA_STDNT_RTN_TIV_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_STDNT_RTN_TIV_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_F_FA_STDNT_RTN_TIV.
--
 --V01  SMT-xxxx 07/11/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_RTN_TIV';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_RTN_TIV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_RTN_TIV';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_RTN_TIV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_RTN_TIV');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_RTN_TIV disable constraint PK_UM_F_FA_STDNT_RTN_TIV';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_RTN_TIV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_RTN_TIV';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_STDNT_RTN_TIV
select /*+ parallel(8) */
S.INSTITUTION INSTITUTION_CD,
S.AID_YEAR,
S.STRM TERM_CD,
S.EMPLID PERSON_ID,
--RTRN_TIV_PGM_TYPE,
S.SRC_SYS_ID,
nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
nvl(E.PERSON_SID,2147483646) PERSON_SID,
RTRN_TIV_STATUS,
NVL((SELECT MIN(X.XLATSHORTNAME)
       FROM UM_D_XLATITEM_VW X
      WHERE X.FIELDNAME = 'RTRN_TIV_STATUS'
        AND X.FIELDVALUE = RTRN_TIV_STATUS),'-') RTRN_TIV_STATUS_SD,
RTRN_TIV_WSTAT_DT,
RTRN_TIV_PERIOD_TP,
NVL((SELECT MIN(X.XLATSHORTNAME)
       FROM UM_D_XLATITEM_VW X
      WHERE X.FIELDNAME = 'RTRN_TIV_PERIOD_TP'
        AND X.FIELDVALUE = RTRN_TIV_PERIOD_TP),'-') RTRN_TIV_PERIOD_TP_SD,
RTRN_TIV_FORM_DT,
WITHDRAW_DATE,
PERIOD_START_DT,
PERIOD_END_DT,
RTRN_TIV_COMP_DAYS,
RTRN_TIV_TOT_DAYS,
RTRN_TIV_CAL_PCT,
TIV_AID_UNEARN_PCT,
RTRN_TIV_DAYS_PAST,
LAST_UPDATE_DTTM,
OPRID,
SFA_HERA_FLG,
SFA_RTIV_CHD_OVRDE,
'N','S',sysdate,sysdate,1234
from CSSTG_OWNER.PS_STDNT_RTN_TIV S
left outer join PS_D_INSTITUTION I
  on S.INSTITUTION = I.INSTITUTION_CD
 and S.SRC_SYS_ID = I.SRC_SYS_ID
left outer join PS_D_PERSON E
  on S.EMPLID = E.PERSON_ID
 and S.SRC_SYS_ID = E.SRC_SYS_ID
where S.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_RTN_TIV rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_RTN_TIV',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_RTN_TIV',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_RTN_TIV';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_RTN_TIV enable constraint PK_UM_F_FA_STDNT_RTN_TIV';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_RTN_TIV');

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

END UM_F_FA_STDNT_RTN_TIV_P;
/
