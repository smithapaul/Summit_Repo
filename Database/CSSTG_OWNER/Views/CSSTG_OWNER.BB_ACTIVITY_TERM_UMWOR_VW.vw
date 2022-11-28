DROP VIEW CSSTG_OWNER.BB_ACTIVITY_TERM_UMWOR_VW
/

--
-- BB_ACTIVITY_TERM_UMWOR_VW  (View) 
--
CREATE OR REPLACE VIEW CSSTG_OWNER.BB_ACTIVITY_TERM_UMWOR_VW
BEQUEATH DEFINER
AS 
with Q1 as (
select /*+ parallel(8) inline no_use_nl(ACT CM U) */
       'UMWOR' AS INSTITUTION_CD,
       CASE WHEN cm.batch_uid LIKE '0%' THEN TO_CHAR(SUBSTR(cm.batch_uid,INSTR(cm.batch_uid,'_',1,3)+1, 4))
            WHEN cm.batch_uid LIKE 'MASTER%' THEN TO_CHAR(SUBSTR(cm.batch_uid,INSTR(cm.batch_uid,'_',1,2)+1, 4))
		END AS TERM_CD,
       CASE WHEN cm.bb_source='UMWOR' AND cm.batch_uid LIKE '0%' THEN SUBSTR(cm.batch_uid,0, INSTR(cm.batch_uid,'_',1)-1)
            WHEN cm.bb_source='UMWOR' AND cm.batch_uid LIKE 'MASTER%' THEN SUBSTR(cm.batch_uid,8, INSTR(cm.batch_uid,'_',1)-1)
            ELSE NULL
        END AS CLASS_NUM,
       case when length(U.BATCH_UID) = 8
             and U.BATCH_UID between '00000000' and '99999999'
            then U.BATCH_UID
            else NULL
        end AS STUDENT_ID,
       ACT.PK1, ACT.EVENT_TYPE, ACT.USER_PK1, ACT.COURSE_PK1, ACT.GROUP_PK1, ACT.FORUM_PK1, ACT.INTERNAL_HANDLE, ACT.CONTENT_PK1,
       ACT.DATA, ACT.TIMESTAMP, ACT.STATUS, ACT.SESSION_ID, CM.BATCH_UID COURSE_BATCH_UID, U.BATCH_UID USER_BATCH_UID,
       ACT.DELETE_FLAG, ACT.INSERT_TIME, ACT.UPDATE_TIME   -- Remove columns not needed
  from CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMWOR ACT
  join CSSTG_OWNER.BB_COURSE_MAIN_S2 CM
    on ACT.COURSE_PK1 = CM.PK1
   and CM.BB_SOURCE = 'UMWOR'
  join CSSTG_OWNER.BB_USERS_S2 U
    on ACT.USER_PK1 = U.PK1
   and U.BB_SOURCE = 'UMWOR'
 where ACT.EVENT_TYPE in ('COURSE_ACCESS'))
select /*+ parallel(8) */
       Q1.INSTITUTION_CD,
       case when not (length(Q1.TERM_CD) = 4 and Q1.TERM_CD between '0000' and '8000') then NULL else Q1.TERM_CD end TERM_CD, Q1.CLASS_NUM, Q1.STUDENT_ID,
       Q1.PK1, Q1.EVENT_TYPE, Q1.USER_PK1, Q1.COURSE_PK1, Q1.GROUP_PK1, Q1.FORUM_PK1, Q1.INTERNAL_HANDLE, Q1.CONTENT_PK1,
       Q1.DATA, Q1.TIMESTAMP, Q1.STATUS, Q1.SESSION_ID, Q1.COURSE_BATCH_UID, Q1.USER_BATCH_UID,
       Q1.DELETE_FLAG, Q1.INSERT_TIME, Q1.UPDATE_TIME   -- Remove columns not needed
  from Q1
/
