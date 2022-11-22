DROP PROCEDURE CSMRT_OWNER.UM_F_COMM_PERSON_P
/

--
-- UM_F_COMM_PERSON_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_COMM_PERSON_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_COMM_PERSON
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_COMM_PERSON';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_COMM_PERSON';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_COMM_PERSON';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_COMM_PERSON';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_COMM_PERSON');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_COMM_PERSON disable constraint PK_UM_F_COMM_PERSON';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_COMM_PERSON';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_COMM_PERSON';
insert /*+ append parallel(8) enable_parallel_dml */ into CSMRT_OWNER.UM_F_COMM_PERSON
with CATG as (
select /*+ parallel(8) inline */
       INSTITUTION,
       COMM_CATEGORY,
       EFFDT,
       SRC_SYS_ID,
       DESCR,
       DESCRSHORT,
       ROW_NUMBER() OVER (PARTITION BY INSTITUTION, COMM_CATEGORY, SRC_SYS_ID
                              ORDER BY EFFDT desc) CATG_ORDER
  from CSSTG_OWNER.PS_COMM_CATG_TBL
 where DATA_ORIGIN <> 'D'
),
CTXT as (
select /*+ parallel(8) inline */
       INSTITUTION,
       COMM_CONTEXT,
       EFFDT,
       SRC_SYS_ID,
       DESCR,
       DESCRSHORT,
       ROW_NUMBER() OVER (PARTITION BY INSTITUTION, COMM_CONTEXT, SRC_SYS_ID
                              ORDER BY EFFDT desc) CTXT_ORDER
  from CSSTG_OWNER.PS_COMM_CTXT_TBL
 where DATA_ORIGIN <> 'D'
)
select /*+ parallel(8) */
       C.COMMON_ID,
       C.SEQ_3C,
       C.SRC_SYS_ID,
       C.INSTITUTION,
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
       nvl(P1.PERSON_SID, 2147483646) PERSON_SID,
       nvl(AF.ADMIN_FUNC_SID, 2147483646) ADMIN_FUNC_SID,
       nvl(D.DEPT_SID, 2147483646) DEPT_FUNC_SID,
       nvl(P2.PERSON_SID, 2147483646) PERSON_ASSIGNED_SID,
       nvl(P3.PERSON_SID, 2147483646) PERSON_COMPLETED_SID,
       nvl(V.VAR_DATA_SID, 2147483646) VAR_DATA_SID,
       C.CHECKLIST_SEQ_3C,
       C.CHECKLIST_SEQ,
       C.COMM_CATEGORY,
       nvl(CATG.DESCRSHORT,'-') COMM_CATEGORY_SD,
       nvl(CATG.DESCR,'-') COMM_CATEGORY_LD,
       C.COMM_CONTEXT,
       nvl(CTXT.DESCRSHORT,'-') COMM_CONTEXT_SD,
       nvl(CTXT.DESCR,'-') COMM_CONTEXT_LD,
       C.COMM_DT,
       C.COMM_DTTM,
       C.COMM_METHOD,
       C.COMPLETED_COMM COMPLETED_COMM_FLG,
       C.COMPLETED_DT,
       C.COMMENT_PRINT_FLAG,
       C.JOINT_COMM JOINT_COMM_FLG,
       C.LETTER_PRINTED_DT,
       C.ORG_CONTACT,
       C.OUTCOME_REASON,
       C.PROCESS_INSTANCE,
       C.SA_ID_TYPE,
       C.SCC_COMM_LANG,
       C.SCC_COMM_MTHD,
       C.SCC_COMM_PROC,
       C.SCC_LETTER_CD,
       nvl(L.DESCRSHORT,'-') SCC_LETTER_SD,
       nvl(L.DESCR,'-') SCC_LETTER_LD,
       C.UNSUCCESSFUL UNSUCCESSFUL_FLG,
       C.VAR_DATA_SEQ,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID,
       C.COMM_COMMENTS
  from CSSTG_OWNER.PS_COMMUNICATION C
  left outer join CATG
    on C.INSTITUTION = CATG.INSTITUTION
   and C.COMM_CATEGORY = CATG.COMM_CATEGORY
   and C.SRC_SYS_ID = CATG.SRC_SYS_ID
   and nvl(CATG_ORDER,1) = 1
  left outer join CTXT
    on C.INSTITUTION = CTXT.INSTITUTION
   and C.COMM_CONTEXT = CTXT.COMM_CONTEXT
   and C.SRC_SYS_ID = CTXT.SRC_SYS_ID
   and nvl(CTXT_ORDER,1) = 1
  left outer join CSSTG_OWNER.PS_SCC_STN_LTR_TBL L
    on C.SCC_LETTER_CD = L.SCC_LETTER_CD
   and C.SRC_SYS_ID = L.SRC_SYS_ID
   and L.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
    on C.INSTITUTION = I.INSTITUTION_CD
   and C.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_PERSON_AGG P1
    on C.COMMON_ID = P1.PERSON_ID
   and C.SRC_SYS_ID = P1.SRC_SYS_ID
   and P1.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ADMIN_FUNC AF
    on C.ADMIN_FUNCTION = AF.ADMIN_FUNCTION
   and C.SRC_SYS_ID = AF.SRC_SYS_ID
   and AF.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_DEPT D
    on C.INSTITUTION = D.SETID
   and C.DEPTID = D.DEPT_ID
   and C.SRC_SYS_ID = D.SRC_SYS_ID
   and D.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_PERSON_AGG P2
    on C.COMM_ID = P2.PERSON_ID
   and C.SRC_SYS_ID = P2.SRC_SYS_ID
   and P2.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_PERSON_AGG P3
    on C.COMPLETED_ID = P3.PERSON_ID
   and C.SRC_SYS_ID = P3.SRC_SYS_ID
   and P3.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_VAR_DATA V
    on C.COMMON_ID = V.COMMON_ID
   and C.VAR_DATA_SEQ = V.VAR_DATA_SEQ
   and C.ADMIN_FUNCTION = V.ADMIN_FUNCTION
   and C.SRC_SYS_ID = V.SRC_SYS_ID
   and V.DATA_ORIGIN <> 'D'
 where C.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_COMM_PERSON rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_COMM_PERSON',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_COMM_PERSON';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_COMM_PERSON enable constraint PK_UM_F_COMM_PERSON';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_COMM_PERSON');

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

END UM_F_COMM_PERSON_P;
/
