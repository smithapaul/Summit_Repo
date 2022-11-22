DROP PROCEDURE CSMRT_OWNER.UM_F_FA_STDNT_PELL_ACTN_P
/

--
-- UM_F_FA_STDNT_PELL_ACTN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_STDNT_PELL_ACTN_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_STDNT_PELL_ACTN.
--
 --V01  SMT-xxxx 07/06/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_PELL_ACTN';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ACTN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ACTN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ACTN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_PELL_ACTN');

----alter table UM_F_FA_STDNT_PELL_ACTN disable constraint PK_UM_F_FA_STDNT_PELL_ACTN;
--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ACTN disable constraint PK_UM_F_FA_STDNT_PELL_ACTN';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_PELL_ACTN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_PELL_ACTN';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_STDNT_PELL_ACTN
with XL as (select /*+ materialize */
                   FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
              from UM_D_XLATITEM
             where SRC_SYS_ID = 'CS90')
select /*+ PARALLEL(8) INLINE */
       A.INSTITUTION INSTITUTION_CD,
       A.AID_YEAR,
       A.EMPLID PERSON_ID,
       A.PELL_ORIG_ID,
       A.PELL_ORIG_ACTN_SEQ,
       nvl(M.PELL_ORIG_MSG_SEQ,0) PELL_ORIG_MSG_SEQ,
       A.SRC_SYS_ID,
       I.INSTITUTION_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       A.PELL_ACTION_CD,
       nvl(X1.XLATSHORTNAME,'-') PELL_ACTION_SD,
       nvl(X1.XLATLONGNAME,'-') PELL_ACTION_LD,
       A.PELL_ACTION_DT,
       A.PELL_BATCH_NBR,
       A.OPRID,
       A.PROCESS_INSTANCE,
       A.SFA_CR_DOCUMENT_ID,
       M.PELL_ORIG_ACTN_MSG,
       'N' LOAD_ERROR, 'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM, 1234 BATCH_SID
  from CSSTG_OWNER.PS_PELL_ORIG_ACTN A
  left outer join CSSTG_OWNER.PS_PELL_ORIG_MSG M
    on A.EMPLID = M.EMPLID
   and A.INSTITUTION = M.INSTITUTION
   and A.AID_YEAR = M.AID_YEAR
   and A.PELL_ORIG_ID = M.PELL_ORIG_ID
   and A.PELL_ORIG_ACTN_SEQ = M.PELL_ORIG_ACTN_SEQ
   and A.SRC_SYS_ID = M.SRC_SYS_ID
   and M.DATA_ORIGIN <> 'D'
  join CSMRT_OWNER.PS_D_INSTITUTION I
    on A.INSTITUTION = I.INSTITUTION_CD
   and A.SRC_SYS_ID = I.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on A.EMPLID = P.PERSON_ID
   and A.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join XL X1
    on X1.FIELDNAME = 'PELL_ACTION_CD'
   and X1.FIELDVALUE = A.PELL_ACTION_CD
   and X1.SRC_SYS_ID = A.SRC_SYS_ID
 where A.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_PELL_ACTN rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_PELL_ACTN',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_PELL_ACTN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ACTN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_FA_STDNT_PELL_ACTN enable constraint PK_UM_F_FA_STDNT_PELL_ACTN;

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_PELL_ACTN enable constraint PK_UM_F_FA_STDNT_PELL_ACTN';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_PELL_ACTN');

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

END UM_F_FA_STDNT_PELL_ACTN_P;
/
