DROP PROCEDURE CSMRT_OWNER.UM_R_STDNT_GRP_P
/

--
-- UM_R_STDNT_GRP_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_R_STDNT_GRP_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_R_STDNT_GRP
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_R_STDNT_GRP';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_R_STDNT_GRP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_R_STDNT_GRP';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_R_STDNT_GRP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_R_STDNT_GRP');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_STDNT_GRP disable constraint PK_UM_R_STDNT_GRP';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_R_STDNT_GRP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_R_STDNT_GRP';
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_R_STDNT_GRP
with HIST as (
select /*+ parallel(8) inline */
       EMPLID PERSON_ID, INSTITUTION INSTITUTION_CD, STDNT_GROUP STDNT_GRP_CD, EFFDT, SRC_SYS_ID,
       EFF_STATUS EFF_STAT_CD, COMMENTS,
       case when EFFDT > SYSDATE then 0 else row_number() over (partition by EMPLID, INSTITUTION, STDNT_GROUP, SRC_SYS_ID
                              order by (case when EFFDT > SYSDATE then to_date('01-JAN-1900') else EFFDT end) desc) end GRP_ORDER
  from CSSTG_OWNER.PS_STDNT_GRPS_HIST
 where DATA_ORIGIN <> 'D'),
G as (
select /*+ parallel(8) inline */
       HIST.INSTITUTION_CD, HIST.PERSON_ID, HIST.STDNT_GRP_CD, HIST.EFFDT, HIST.SRC_SYS_ID,
       HIST.EFF_STAT_CD,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       nvl(D.STDNT_GRP_SID,2147483646) STDNT_GRP_SID,
       decode(GRP_ORDER,1,'Y','N') CURRENT_IND,
       HIST.COMMENTS
  from HIST
  join PS_D_PERSON P
    on HIST.PERSON_ID = P.PERSON_ID
   and HIST.SRC_SYS_ID = P.SRC_SYS_ID
  join PS_D_STDNT_GRP D
    on HIST.INSTITUTION_CD = D.INSTITUTION_CD
   and HIST.STDNT_GRP_CD = D.STDNT_GRP_CD
   and HIST.SRC_SYS_ID = D.SRC_SYS_ID),
GRP as (
SELECT /*+ parallel(8) inline */
                S.TERM_SID,
                S.PERSON_SID,
                G.STDNT_GRP_SID,
                S.SRC_SYS_ID,
                S.INSTITUTION_CD,
                S.ACAD_CAR_CD,
                S.TERM_CD,
                S.PERSON_ID,
                G.STDNT_GRP_CD,
                G.EFFDT,
                G.EFF_STAT_CD,
--                T.EFF_START_DT,
                T.TERM_BEGIN_DT EFF_START_DT,   -- Changed May 2016
                T.EFF_END_DT,
                G.CURRENT_IND,
                G.COMMENTS,
                ROW_NUMBER ()
                   OVER (PARTITION BY S.TERM_SID,
                                      S.PERSON_SID,
                                      G.STDNT_GRP_SID,
                                      S.SRC_SYS_ID
                             ORDER BY G.EFFDT DESC) GRP_TERM_ORDER
           FROM UM_F_STDNT_TERM S
           JOIN PS_D_TERM T ON S.TERM_SID = T.TERM_SID
           LEFT OUTER JOIN G
             ON G.PERSON_SID = S.PERSON_SID
            AND G.SRC_SYS_ID = S.SRC_SYS_ID
            AND G.INSTITUTION_CD = T.INSTITUTION_CD
            AND G.EFFDT <= T.TERM_END_DT
--              AND G.EFFDT <= T.TERM_BEGIN_DT    -- Fixed APR 2015    -- Backed out Aug 2016
            AND G.SRC_SYS_ID = S.SRC_SYS_ID),
GRP2 as (
SELECT /*+ parallel(8) inline */ distinct
       TERM_SID,
       PERSON_SID,
       case when nvl(EFF_STAT_CD,'-') =  'A' then STDNT_GRP_SID
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then STDNT_GRP_SID
       else 2147483646 end STDNT_GRP_SID,
       SRC_SYS_ID,
       INSTITUTION_CD,
       ACAD_CAR_CD,
       TERM_CD,
       PERSON_ID,
       case when nvl(EFF_STAT_CD,'-') =  'A' then STDNT_GRP_CD
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then STDNT_GRP_CD
       else '-' end STDNT_GRP_CD,
       case when nvl(EFF_STAT_CD,'-') =  'A' then EFFDT
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then EFFDT
       else NULL end EFFDT,
       case when nvl(EFF_STAT_CD,'-') =  'A' then EFF_STAT_CD
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then EFF_STAT_CD
       else '-' end EFF_STAT_CD,
       EFF_START_DT,
       EFF_END_DT,
       case when nvl(EFF_STAT_CD,'-') =  'A' then CURRENT_IND
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then CURRENT_IND
       else '-' end CURRENT_IND,
          (CASE WHEN INSTITUTION_CD = 'UMBOS'
                 AND STDNT_GRP_CD = 'HNRS'
                 AND EFF_STAT_CD = 'A'
                THEN 'Y'
                WHEN INSTITUTION_CD = 'UMDAR'
                 AND STDNT_GRP_CD = 'HNRS'
                 AND EFF_STAT_CD = 'A'
                THEN 'Y'
                WHEN INSTITUTION_CD = 'UMLOW'
                 AND STDNT_GRP_CD = 'SRHN'
                 AND EFF_STAT_CD = 'A'
                THEN 'Y'
                ELSE 'N'
            END) HONORS_FLG,
       case when nvl(EFF_STAT_CD,'-') =  'A' then ROW_NUMBER() OVER (PARTITION BY PERSON_SID, TERM_SID, SRC_SYS_ID ORDER BY STDNT_GRP_CD)
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then ROW_NUMBER() OVER (PARTITION BY PERSON_SID, TERM_SID, SRC_SYS_ID ORDER BY STDNT_GRP_CD)
       else 1 end STDNT_GROUP_ORDER,
       case when nvl(EFF_STAT_CD,'-') =  'A' then COMMENTS
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then COMMENTS
       else '' end COMMENTS
  FROM GRP
 WHERE GRP_TERM_ORDER = 1
),
GRP3 as (
SELECT /*+ parallel(8) inline */
       TERM_SID,
       PERSON_SID,
       STDNT_GRP_SID,
       SRC_SYS_ID,
       INSTITUTION_CD,
       ACAD_CAR_CD,
       TERM_CD,
       PERSON_ID,
       STDNT_GRP_CD,
       EFFDT,
       EFF_STAT_CD,
       EFF_START_DT,
       EFF_END_DT,
       CURRENT_IND,
       HONORS_FLG,
       STDNT_GROUP_ORDER,
       COMMENTS,
--       row_number() over (partition by TERM_SID, PERSON_SID, SRC_SYS_ID
--                              order by decode(EFF_STAT_CD,'A',0,'I',1,9),STDNT_GRP_CD) STDNT_TERM_ORDER                  -- Oct 2017
       dense_rank() over (partition by TERM_SID, PERSON_SID, SRC_SYS_ID
                              order by decode(STDNT_GRP_CD,'-',9,0)) STDNT_TERM_ORDER                  -- Feb 2018
  from GRP2
)
select /*+ parallel(8) */
       TERM_SID,
       PERSON_SID,
       STDNT_GRP_SID,
       SRC_SYS_ID,
       INSTITUTION_CD,
       ACAD_CAR_CD,
       TERM_CD,
       PERSON_ID,
       STDNT_GRP_CD,
       EFFDT,
       EFF_STAT_CD,
       EFF_START_DT,
       EFF_END_DT,
       CURRENT_IND,
       HONORS_FLG,
       STDNT_GROUP_ORDER,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID,
       COMMENTS
  from GRP3
 where (EFF_STAT_CD = 'A' or STDNT_TERM_ORDER = 1)          -- Oct 2017
;
commit;
merge /* parallel(8) */ /*+ use_hash(G,T) */ into CSMRT_OWNER.UM_R_STDNT_GRP G
using (
with HIST as (
select /*+ parallel(8) inline */
       EMPLID PERSON_ID, INSTITUTION INSTITUTION_CD, STDNT_GROUP STDNT_GRP_CD, EFFDT, SRC_SYS_ID,
       EFF_STATUS EFF_STAT_CD, COMMENTS,
       case when EFFDT > SYSDATE then 0 else row_number() over (partition by EMPLID, INSTITUTION, STDNT_GROUP, SRC_SYS_ID
                              order by (case when EFFDT > SYSDATE then to_date('01-JAN-1900') else EFFDT end) desc) end GRP_ORDER
  from CSSTG_OWNER.PS_STDNT_GRPS_HIST
 where DATA_ORIGIN <> 'D'),
G as (
select /*+ parallel(8) inline */
       HIST.INSTITUTION_CD, HIST.PERSON_ID, HIST.STDNT_GRP_CD, HIST.EFFDT, HIST.SRC_SYS_ID,
       HIST.EFF_STAT_CD,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       nvl(D.STDNT_GRP_SID,2147483646) STDNT_GRP_SID,
       decode(GRP_ORDER,1,'Y','N') CURRENT_IND,
       HIST.COMMENTS
  from HIST
  join PS_D_PERSON P
    on HIST.PERSON_ID = P.PERSON_ID
   and HIST.SRC_SYS_ID = P.SRC_SYS_ID
  join PS_D_STDNT_GRP D
    on HIST.INSTITUTION_CD = D.INSTITUTION_CD
   and HIST.STDNT_GRP_CD = D.STDNT_GRP_CD
   and HIST.SRC_SYS_ID = D.SRC_SYS_ID),
GRP
     AS (SELECT /*+ INLINE PARALLEL(8) */
                S.ADMIT_TERM_SID TERM_SID,
                S.APPLCNT_SID PERSON_SID,
                nvl(G.STDNT_GRP_SID,2147483646) STDNT_GRP_SID,
                S.SRC_SYS_ID,
                T.INSTITUTION_CD,
                T.ACAD_CAR_CD,
                T.TERM_CD,
                nvl(G.PERSON_ID,'-') PERSON_ID,
                nvl(G.STDNT_GRP_CD,'-') STDNT_GRP_CD,
                nvl(G.EFFDT, to_date('01-JAN-1900')) EFFDT,
                nvl(G.EFF_STAT_CD,'-') EFF_STAT_CD,
--                T.EFF_START_DT,
                T.TERM_BEGIN_DT EFF_START_DT,   -- Changed May 2016
                T.EFF_END_DT,
                G.CURRENT_IND,
                G.COMMENTS,
                ROW_NUMBER ()
                   OVER (PARTITION BY S.ADMIT_TERM_SID,
                                      S.APPLCNT_SID,
                                      G.STDNT_GRP_SID,
                                      S.SRC_SYS_ID
                         ORDER BY G.EFFDT DESC)
                   GRP_TERM_ORDER
           FROM UM_F_ADM_APPL_STAT S
           JOIN PS_D_TERM T ON S.ADMIT_TERM_SID = T.TERM_SID
           LEFT OUTER JOIN G
             ON G.PERSON_SID = S.APPLCNT_SID
            AND G.SRC_SYS_ID = S.SRC_SYS_ID
            AND G.INSTITUTION_CD = T.INSTITUTION_CD
            AND G.EFFDT <= T.TERM_END_DT
--            AND G.EFFDT <= T.TERM_BEGIN_DT    -- Fixed APR 2015    -- Backed out Aug 2016
            AND G.SRC_SYS_ID = S.SRC_SYS_ID),
GRP2 as (
SELECT /*+ parallel(8) inline */ distinct
       TERM_SID,
       PERSON_SID,
       case when nvl(EFF_STAT_CD,'-') =  'A' then STDNT_GRP_SID
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then STDNT_GRP_SID
       else 2147483646 end STDNT_GRP_SID,
       SRC_SYS_ID,
       INSTITUTION_CD,
       ACAD_CAR_CD,
       TERM_CD,
       PERSON_ID,
       case when nvl(EFF_STAT_CD,'-') =  'A' then STDNT_GRP_CD
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then STDNT_GRP_CD
       else '-' end STDNT_GRP_CD,
       case when nvl(EFF_STAT_CD,'-') =  'A' then EFFDT
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then EFFDT
       else NULL end EFFDT,
       case when nvl(EFF_STAT_CD,'-') =  'A' then EFF_STAT_CD
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then EFF_STAT_CD
       else '-' end EFF_STAT_CD,
       EFF_START_DT,
       EFF_END_DT,
       case when nvl(EFF_STAT_CD,'-') =  'A' then CURRENT_IND
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then CURRENT_IND
       else '-' end CURRENT_IND,
          (CASE WHEN INSTITUTION_CD = 'UMBOS'
                 AND STDNT_GRP_CD = 'HNRS'
                 AND EFF_STAT_CD = 'A'
                THEN 'Y'
                WHEN INSTITUTION_CD = 'UMDAR'
                 AND STDNT_GRP_CD = 'HNRS'
                 AND EFF_STAT_CD = 'A'
                THEN 'Y'
                WHEN INSTITUTION_CD = 'UMLOW'
                 AND STDNT_GRP_CD = 'SRHN'
                 AND EFF_STAT_CD = 'A'
                THEN 'Y'
                ELSE 'N'
            END) HONORS_FLG,
       case when nvl(EFF_STAT_CD,'-') =  'A' then COMMENTS
            when nvl(EFF_STAT_CD,'-') <> 'A' and nvl(EFFDT,to_date('01-JAN-1900')) between EFF_START_DT AND EFF_END_DT then COMMENTS
       else '' end COMMENTS
  FROM GRP
 WHERE GRP_TERM_ORDER = 1
   and not ((STDNT_GRP_CD = '-' or EFF_STAT_CD <> 'A') and exists (select 1 from UM_R_STDNT_GRP R where GRP.TERM_SID = R.TERM_SID and GRP.PERSON_SID = R.PERSON_SID)) -- Oct 2017
),
GRP3 as (
SELECT /*+ parallel(8) inline */
       TERM_SID,
       PERSON_SID,
       STDNT_GRP_SID,
       SRC_SYS_ID,
       INSTITUTION_CD,
       ACAD_CAR_CD,
       TERM_CD,
       PERSON_ID,
       STDNT_GRP_CD,
       EFFDT,
       EFF_STAT_CD,
       EFF_START_DT,
       EFF_END_DT,
       CURRENT_IND,
       HONORS_FLG,
       COMMENTS,
--       row_number() over (partition by TERM_SID, PERSON_SID, SRC_SYS_ID
--                              order by decode(EFF_STAT_CD,'A',0,'I',1,9),STDNT_GRP_CD) STDNT_TERM_ORDER                  -- Oct 2017
       dense_rank() over (partition by TERM_SID, PERSON_SID, SRC_SYS_ID
                              order by decode(STDNT_GRP_CD,'-',9,0)) STDNT_TERM_ORDER                  -- Feb 2018
  from GRP2
)
select /*+ parallel(8) */
       TERM_SID,
       PERSON_SID,
       STDNT_GRP_SID,
       SRC_SYS_ID,
       INSTITUTION_CD,
       ACAD_CAR_CD,
       TERM_CD,
       PERSON_ID,
       STDNT_GRP_CD,
       EFFDT,
       EFF_STAT_CD,
       EFF_START_DT,
       EFF_END_DT,
       CURRENT_IND,
       HONORS_FLG,
       ROW_NUMBER() OVER (PARTITION BY PERSON_SID, TERM_SID, SRC_SYS_ID ORDER BY STDNT_GRP_CD) STDNT_GRP_ORDER,   -- Feb 2018
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID,
       COMMENTS
  from GRP3
-- where (EFF_STAT_CD = 'A' or STDNT_TERM_ORDER = 1)          -- Oct 2017
 where STDNT_TERM_ORDER = 1          -- Feb 2018
) T
   on (G.TERM_SID         = T.TERM_SID
  and  G.PERSON_SID       = T.PERSON_SID
  and  G.STDNT_GRP_SID    = T.STDNT_GRP_SID
  and  G.SRC_SYS_ID       = T.SRC_SYS_ID)
--when matched then update set
--where
when not matched then
insert (
G.TERM_SID,
G.PERSON_SID,
G.STDNT_GRP_SID,
G.SRC_SYS_ID,
G.INSTITUTION_CD,
G.ACAD_CAR_CD,
G.TERM_CD,
G.PERSON_ID,
G.STDNT_GRP_CD,
G.EFFDT,
G.EFF_STAT_CD,
G.EFF_START_DT,
G.EFF_END_DT,
G.CURRENT_IND,
G.HONORS_FLG,
G.STDNT_GRP_ORDER,
G.LOAD_ERROR,
G.DATA_ORIGIN,
G.CREATED_EW_DTTM,
G.LASTUPD_EW_DTTM,
G.BATCH_SID,
G.COMMENTS
)
values (
T.TERM_SID,
T.PERSON_SID,
T.STDNT_GRP_SID,
T.SRC_SYS_ID,
T.INSTITUTION_CD,
T.ACAD_CAR_CD,
T.TERM_CD,
T.PERSON_ID,
T.STDNT_GRP_CD,
T.EFFDT,
T.EFF_STAT_CD,
T.EFF_START_DT,
T.EFF_END_DT,
T.CURRENT_IND,
T.HONORS_FLG,
T.STDNT_GRP_ORDER,
'N',
'S',
sysdate,
sysdate,
1234,
T.COMMENTS)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_R_STDNT_GRP rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_R_STDNT_GRP',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_R_STDNT_GRP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_STDNT_GRP enable constraint PK_UM_R_STDNT_GRP';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_R_STDNT_GRP');

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

END UM_R_STDNT_GRP_P;
/
