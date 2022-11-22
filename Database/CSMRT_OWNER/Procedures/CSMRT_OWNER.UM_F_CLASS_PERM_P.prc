DROP PROCEDURE CSMRT_OWNER.UM_F_CLASS_PERM_P
/

--
-- UM_F_CLASS_PERM_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_CLASS_PERM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_CLASS_PERM.
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_CLASS_PERM';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_CLASS_PERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_CLASS_PERM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_CLASS_PERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_CLASS_PERM');

----alter table UM_F_CLASS_PERM disable constraint PK_UM_F_CLASS_PERM;
--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_CLASS_PERM disable constraint PK_UM_F_CLASS_PERM';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_CLASS_PERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_CLASS_PERM';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_CLASS_PERM
  with XL as (
select /*+ inline parallel(8) no_merge */ 
       FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
  from UM_D_XLATITEM
 where SRC_SYS_ID = 'CS90')
select /*+ inline parallel(8) no_merge */ 
       PERM.CRSE_ID CRSE_CD,                -- Class PK
       PERM.CRSE_OFFER_NBR CRSE_OFFER_NUM,  -- Class PK
       PERM.STRM TERM_CD,                   -- Class PK
       PERM.SESSION_CODE SESSION_CD,        -- Class PK
       PERM.CLASS_SECTION CLASS_SECTION_CD, -- Class PK
       PERM.PERMISSION_TYPE,                -- XLAT Lookup
       PERM.CLASS_PRMSN_SEQ,                -- Max 2000
       PERM.SRC_SYS_ID,                     -- Class PK
       PERM.CLASS_PRMSN_NBR,                -- Max 999999
       nvl(C.CLASS_SID,2147483646) CLASS_SID,
       nvl(X1.XLATSHORTNAME,'-') PERMISSION_TYPE_SD,
       nvl(X1.XLATLONGNAME,'-') PERMISSION_TYPE_LD,
       PERM.PERMISSION_USED,                -- XLAT Lookup
       nvl(X2.XLATSHORTNAME,'-') PERMISSION_USED_SD,
       nvl(X2.XLATLONGNAME,'-') PERMISSION_USED_LD,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
--       EMPLID,                            -- UM_D_PERSON Lookup
       PERM.PERMISSION_USE_DT,
       PERM.PRMSN_EXPIRE_DT,
       PERM.OPRID,
       case when CREATION_DT is not NULL
             and CREATION_TIME is not NULL
            then to_date(to_char(trunc(CREATION_DT),'YYYYMMDD')||' '||to_char(CREATION_TIME,'HH24:MI:SS'),'YYYYMMDD HH24:MI:SS')
            else NULL
        end CREATION_DT,
--       CREATION_DT,
--       CREATION_TIME,       -- Merge with CREATION_DT???
       PERM.OPRID_LAST_UPDT,
       case when LAST_UPD_DT_STMP is not NULL
             and LAST_UPD_TM_STMP is not NULL
            then to_date(to_char(trunc(LAST_UPD_DT_STMP),'YYYYMMDD')||' '||to_char(LAST_UPD_TM_STMP,'HH24:MI:SS'),'YYYYMMDD HH24:MI:SS')
            else NULL
        end LAST_UPD_DT,
--       LAST_UPD_DT_STMP,
--       LAST_UPD_TM_STMP,    -- Merge with LAST_UPD_DT_STMP??? Same for 2.8 M rows!!!
       PERM.SSR_ISSUE_FL,        -- Flag
       PERM.SSR_ISSUE_OPRID,
       case when SSR_ISSUE_DT is not NULL
             and SSR_ISSUE_TIME is not NULL
            then to_date(to_char(trunc(SSR_ISSUE_DT),'YYYYMMDD')||' '||to_char(SSR_ISSUE_TIME,'HH24:MI:SS'),'YYYYMMDD HH24:MI:SS')
            else NULL
        end SSR_ISSUE_DT,
--       SSR_ISSUE_DT,
--       SSR_ISSUE_TIME,      -- Merge with CREATION_DT???
       PERM.OVRD_CAREER,         -- Flag
       PERM.OVRD_CLASS_LIMIT,    -- Flag
       PERM.SSR_OVRD_CONSENT,    -- Flag. Only one 'N' row.
       PERM.SSR_OVRD_REQ,        -- Flag. Only one 'N' row.
       PERM.SSR_OVRD_TIME_PERD,  -- Flag
--       DESCR50              -- Empty. Two rows with values.
       sum(1) over (partition by PERM.CRSE_ID, PERM.CRSE_OFFER_NBR, PERM.STRM, PERM.SESSION_CODE, PERM.CLASS_SECTION, PERM.SRC_SYS_ID) PERM_CNT,
       sum(decode(PERMISSION_USED,'N',1,0)) over (partition by PERM.CRSE_ID, PERM.CRSE_OFFER_NBR, PERM.STRM, PERM.SESSION_CODE, PERM.CLASS_SECTION, PERM.SRC_SYS_ID) AVAIL_CNT,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  from CSSTG_OWNER.PS_CLASS_PRMSN PERM
  left outer join UM_D_CLASS C
    on PERM.CRSE_ID = C.CRSE_CD
   and PERM.CRSE_OFFER_NBR = C.CRSE_OFFER_NUM
   and PERM.STRM = C.TERM_CD
   and PERM.SESSION_CODE = C.SESSION_CD
   and PERM.CLASS_SECTION = C.CLASS_SECTION_CD
   and PERM.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join PS_D_PERSON P
    on PERM.EMPLID = P.PERSON_ID
   and PERM.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join XL X1
    on X1.FIELDNAME = 'PERMISSION_TYPE'
   and X1.FIELDVALUE = PERM.PERMISSION_TYPE
  left outer join XL X2
    on X2.FIELDNAME = 'PERMISSION_USED'
   and X2.FIELDVALUE = PERM.PERMISSION_USED
 where PERM.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_CLASS_PERM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_CLASS_PERM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_CLASS_PERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_CLASS_PERM enable constraint PK_UM_F_CLASS_PERM';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_CLASS_PERM');

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

END UM_F_CLASS_PERM_P;
/
