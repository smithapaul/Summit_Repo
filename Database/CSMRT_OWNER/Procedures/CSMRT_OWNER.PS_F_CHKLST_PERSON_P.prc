CREATE OR REPLACE PROCEDURE             "PS_F_CHKLST_PERSON_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table               -- PS_F_CHKLST_PERSON
--PS_F_CHKLST_PERSON    -- PS_D_ADMIN_FUNC;PS_D_CHKLST_CD;PS_D_CHKLST_STAT;PS_D_DEPT;PS_D_INSTITUTION;PS_D_PERSON;PS_D_VAR_DATA;
--V01 11/30/2018            -- srikanth ,pabbu converted to proc from sql

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_F_CHKLST_PERSON';
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

strMessage01    := 'Truncating table CSMRT_OWNER.PS_F_CHKLST_PERSON';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_F_CHKLST_PERSON';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_F_CHKLST_PERSON';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_F_CHKLST_PERSON');

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_CHKLST_PERSON disable constraint PK_PS_F_CHKLST_PERSON';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.PS_F_CHKLST_PERSON';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_F_CHKLST_PERSON';
insert /*+ append parallel(8) enable_parallel_dml */ into PS_F_CHKLST_PERSON
SELECT /*+ parallel(8) */
       PC.COMMON_ID,
       PC.SEQ_3C,
       PC.SRC_SYS_ID,
	   PC.INSTITUTION INSTITUTION_CD,
	   nvl(A.ADMIN_FUNC_SID, 2147483646) ADMIN_FUNC_SID,
	   nvl(C.CHKLIST_CD_SID, 2147483646) CHKLIST_CD_SID,
	   nvl(S.CHKLIST_STAT_SID, 2147483646) CHKLIST_STAT_SID,
	   nvl(D.DEPT_SID, 2147483646) DEPT_SID,
	   nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
	   nvl(P.PERSON_SID, 2147483646) PERSON_SID,
	   nvl(PR.PERSON_SID, 2147483646) RESPONSIBLE_SID,
	   nvl(PS.PERSON_SID, 2147483646) STAT_CHG_SID,
	   nvl(PD.VAR_DATA_SID, 2147483646) VAR_DATA_SID,
       trunc(PC.CHECKLIST_DTTM) CHECKLIST_DT,
	   PC.CHECKLIST_DTTM CHKLIST_TM,
       trunc(PC.DUE_DT) DUE_DT,
       trunc(PC.STATUS_DT) STATUS_DT,
       PC.TRACKING_SEQ,
       PC.CURRENCY_CD,
       PC.DUE_AMT,
--	   PC.ADMIN_FUNCTION ADMIN_FUNC_AREA,
       case when PC.ADMIN_FUNCTION in ('ADMA', 'ADMP')
            then 'ADM'
            when PC.ADMIN_FUNCTION in ('AVAK', 'AVIN', 'AVMB', 'AVMS')
            then 'CR'
            when PC.ADMIN_FUNCTION in ('AWRD', 'BDGT', 'FINA', 'ISIR', 'LOAN', 'RSTR')
            then 'FA'
            when PC.ADMIN_FUNCTION in ('PROP', 'PROS', 'PSSV', 'RECR')
            then 'REC'
            when PC.ADMIN_FUNCTION in ('SFAC', 'SFBI', 'SFCO', 'SFGR', 'SFIT', 'SFPA', 'SFPR', 'SFRC', 'SFRF', 'SFTP')
            then 'SF'
            when PC.ADMIN_FUNCTION in ('SPRG', 'STRM')
            then 'SR'
            when PC.ADMIN_FUNCTION in ('EVNT', 'GEN', 'IHC', 'NLBP', 'NLOW')
            then PC.ADMIN_FUNCTION
            else 'OTR'
        end ADMIN_FUNC_AREA,
        ROW_NUMBER () OVER (PARTITION BY PC.COMMON_ID, PC.CHECKLIST_CD, PC.SRC_SYS_ID
                                ORDER BY PC.SEQ_3C DESC) CHKLIST_ORDER,
--        'N' LOAD_ERROR,
        'S' DATA_ORIGIN,
        SYSDATE CREATED_EW_DTTM,
        SYSDATE LASTUPD_EW_DTTM,
--        1234 BATCH_SID
        PC.COMM_COMMENTS        -- Dec 2019
  FROM CSSTG_OWNER.PS_PERSON_CHECKLST  PC  -- NK --> COMMON_ID, SEQ_3C, SRC_SYS_I
  left outer join CSMRT_OWNER.PS_D_ADMIN_FUNC A
   on PC.ADMIN_FUNCTION = A.ADMIN_FUNCTION
   and PC.SRC_SYS_ID = A.SRC_SYS_ID
   and A.DATA_ORIGIN <> 'D'
   left outer join CSMRT_OWNER.PS_D_CHKLST_CD C
    on PC.CHECKLIST_CD = C.CHECKLIST_CD
   and PC.INSTITUTION=C.INSTITUTION_CD
   and PC.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
   left outer join CSMRT_OWNER.PS_D_CHKLST_STAT S
    on PC.CHECKLIST_STATUS = S.CHKLIST_STAT_ID
   and PC.SRC_SYS_ID = S.SRC_SYS_ID
   and S.DATA_ORIGIN <> 'D'
   left outer join CSMRT_OWNER.PS_D_DEPT D
    on PC.DEPTID = D.DEPT_ID
   and PC.INSTITUTION=D.SETID
   and PC.SRC_SYS_ID = D.SRC_SYS_ID
   and D.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
   on PC.INSTITUTION = I.INSTITUTION_CD
   and PC.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on PC.COMMON_ID = P.PERSON_ID
   and PC.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_PERSON PR
    on PC.RESPONSIBLE_ID = PR.PERSON_ID
   and PC.SRC_SYS_ID = PR.SRC_SYS_ID
   and PR.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_PERSON PS
    on PC.STATUS_CHANGE_ID = PS.PERSON_ID
   and PC.SRC_SYS_ID = PS.SRC_SYS_ID
   and PS.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_VAR_DATA PD
    on PC.COMMON_ID = PD.COMMON_ID
   and PC.VAR_DATA_SEQ=PD.VAR_DATA_SEQ
   and PC.ADMIN_FUNCTION=PD.ADMIN_FUNCTION
   and PC.SRC_SYS_ID = PD.SRC_SYS_ID
   and PD.DATA_ORIGIN <> 'D'
 WHERE PC.DATA_ORIGIN <> 'D'
   AND PC.SA_ID_TYPE = 'P'
   AND PC.COMMON_ID between '00000000' and '99999999'
   AND length(trim(PC.COMMON_ID)) = 8
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_F_CHKLST_PERSON rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_F_CHKLST_PERSON',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_F_CHKLST_PERSON';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_CHKLST_PERSON enable constraint PK_PS_F_CHKLST_PERSON';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_F_CHKLST_PERSON');

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

END PS_F_CHKLST_PERSON_P;
/
