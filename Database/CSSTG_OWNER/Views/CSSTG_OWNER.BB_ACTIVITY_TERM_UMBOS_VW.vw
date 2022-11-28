DROP VIEW CSSTG_OWNER.BB_ACTIVITY_TERM_UMBOS_VW
/

--
-- BB_ACTIVITY_TERM_UMBOS_VW  (View) 
--
CREATE OR REPLACE VIEW CSSTG_OWNER.BB_ACTIVITY_TERM_UMBOS_VW
BEQUEATH DEFINER
AS 
with Q1 as (
select /*+ parallel(8) inline no_use_nl(ACT CM U) */
       'UMBOS' AS INSTITUTION_CD,
       case when CM.BATCH_UID like 'B%'
            then to_char(replace(substr(CM.BATCH_UID,2, instr(CM.BATCH_UID,'-',1)-2), '_recover', ''))
            else to_char(substr(CM.BATCH_UID,1, instr(CM.BATCH_UID,'-',1)-2))
        end as TERM_CD,
       case when CM.BATCH_UID like 'B%'
            then substr(CM.BATCH_UID,7,(decode(instr(CM.BATCH_UID,'_',1),0,5,instr(CM.BATCH_UID,'_',1)-7)))
            else NULL
        end as CLASS_NUM,
       case when length(U.BATCH_UID) = 8
             and U.BATCH_UID between '00000000' and '99999999'
            then U.BATCH_UID
            else NULL
        end AS STUDENT_ID,
       ACT.PK1, ACT.EVENT_TYPE, ACT.USER_PK1, ACT.COURSE_PK1, ACT.GROUP_PK1, ACT.FORUM_PK1, ACT.INTERNAL_HANDLE, ACT.CONTENT_PK1,
       ACT.DATA, ACT.TIMESTAMP, ACT.STATUS, ACT.SESSION_ID, CM.BATCH_UID COURSE_BATCH_UID, U.BATCH_UID USER_BATCH_UID,
       ACT.DELETE_FLAG, ACT.INSERT_TIME, ACT.UPDATE_TIME   -- Remove columns not needed
  from CSSTG_OWNER.BB_ACTIVITY_ACCUMULATOR_UMBOS ACT
  join CSSTG_OWNER.BB_COURSE_MAIN_S2 CM
    on ACT.COURSE_PK1 = CM.PK1
   and CM.BB_SOURCE = 'UMBOS'
  join CSSTG_OWNER.BB_USERS_S2 U
    on ACT.USER_PK1 = U.PK1
   and U.BB_SOURCE = 'UMBOS'
 where ACT.EVENT_TYPE in ('COURSE_ACCESS'))
select /*+ parallel(8) */
       Q1.INSTITUTION_CD,
       case when not (length(Q1.TERM_CD) = 4 and Q1.TERM_CD between '2000' and '5000') then NULL else Q1.TERM_CD end TERM_CD, Q1.CLASS_NUM, Q1.STUDENT_ID,
       Q1.PK1, Q1.EVENT_TYPE, Q1.USER_PK1, Q1.COURSE_PK1, Q1.GROUP_PK1, Q1.FORUM_PK1, Q1.INTERNAL_HANDLE, Q1.CONTENT_PK1,
       Q1.DATA, Q1.TIMESTAMP, Q1.STATUS, Q1.SESSION_ID, Q1.COURSE_BATCH_UID, Q1.USER_BATCH_UID,
       Q1.DELETE_FLAG, Q1.INSERT_TIME, Q1.UPDATE_TIME   -- Remove columns not needed
  from Q1
/
