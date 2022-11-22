DROP PROCEDURE CSMRT_OWNER.PS_R_CHKLST_ITEM_P
/

--
-- PS_R_CHKLST_ITEM_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_R_CHKLST_ITEM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- PS_R_CHKLST_ITEM
--V01 01/31/2019             -- Jim Doucette converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_R_CHKLST_ITEM';
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

strMessage01    := 'Truncating table CSMRT_OWNER.PS_R_CHKLST_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_R_CHKLST_ITEM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_R_CHKLST_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_R_CHKLST_ITEM');

--strSqlDynamic   := 'alter table CSMRT_OWNER.PS_R_CHKLST_ITEM disable constraint PK_PS_R_CHKLST_ITEM';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.PS_R_CHKLST_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_R_CHKLST_ITEM';				
insert /*+ append enable_parallel_dml parallel(8) */ into PS_R_CHKLST_ITEM
select /*+ parallel(8) inline */  
       H.COMMON_ID, 
       H.SEQ_3C, 
       D.CHECKLIST_SEQ, 
       H.SRC_SYS_ID,        -- Moved 
       H.INSTITUTION,       -- Added
       I.INSTITUTION_SID,   -- Added lookup   
       nvl(P.PERSON_SID,2147483646) PERSON_SID,        -- Added lookup 
       nvl(C.ITEM_CD_SID,2147483646) ITEM_CD_SID,      -- Lookup to PS_D_ITEM_CD 
       --D.CHKLST_ITEM_CD, 
       D.ITEM_STATUS, 
       nvl(X1.XLATSHORTNAME, '-') ITEM_STATUS_SD, 
       nvl(X1.XLATLONGNAME, '-') ITEM_STATUS_LD,
       D.STATUS_DT, 
       D.STATUS_CHANGE_ID, 
       D.DUE_DT, 
       D.CURRENCY_CD, 
       D.DUE_AMT, 
       D.RESPONSIBLE_ID, 
       D.ASSOC_ID, 
       NAME, 
       COMM_KEY,
       ROW_NUMBER() OVER (PARTITION BY H.COMMON_ID, D.CHKLST_ITEM_CD, H.SRC_SYS_ID
                              ORDER BY H.SEQ_3C DESC, D.CHECKLIST_SEQ DESC) ITEM_ORDER,     -- Added from view 
       'N' LOAD_ERROR, 
       'S' DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM, 
       1234 BATCH_SID
  from CSSTG_OWNER.PS_PERSON_CHECKLST H
  join CSSTG_OWNER.PS_PERSON_CHK_ITEM D 
    on H.COMMON_ID = D.COMMON_ID  
   and H.SEQ_3C = D.SEQ_3C
   and H.SRC_SYS_ID = D.SRC_SYS_ID
   and D.DATA_ORIGIN <> 'D'
  join PS_D_INSTITUTION I
    on H.INSTITUTION = I.INSTITUTION_CD
   and H.SRC_SYS_ID = I.SRC_SYS_ID
  left outer join PS_D_PERSON P
    on H.COMMON_ID = P.PERSON_ID
   and H.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join PS_D_ITEM_CD C
    on D.CHKLST_ITEM_CD = C.CHKLST_ITEM_CD
   and D.SRC_SYS_ID = C.SRC_SYS_ID
  left outer join CSMRT_OWNER.UM_D_XLATITEM X1
    on D.ITEM_STATUS = X1.FIELDVALUE
   and D.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'ITEM_STATUS' 
 where H.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_R_CHKLST_ITEM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_R_CHKLST_ITEM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_R_CHKLST_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.PS_R_CHKLST_ITEM enable constraint PK_PS_R_CHKLST_ITEM';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_R_CHKLST_ITEM');

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

END PS_R_CHKLST_ITEM_P;
/
