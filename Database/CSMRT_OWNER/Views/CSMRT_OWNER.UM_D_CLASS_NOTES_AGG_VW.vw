CREATE OR REPLACE VIEW UM_D_CLASS_NOTES_AGG_VW
BEQUEATH DEFINER
AS 
with N1 as (
select --CLASS_NOTES_SID,
       CRSE_CD,
       CRSE_OFFER_NUM,
       TERM_CD,
       SESSION_CD,
       CLASS_SECTION_CD,
       CLASS_NOTES_SEQ,
       SRC_SYS_ID,
       CLASS_SID,
       DESCRLONG,
       row_number() over (partition by CRSE_CD, CRSE_OFFER_NUM, TERM_CD, SESSION_CD, CLASS_SECTION_CD, SRC_SYS_ID
                              order by CLASS_NOTES_SEQ) NOTE_ORDER
  from UM_D_CLASS_NOTES 
 where DATA_ORIGIN <> 'D')
select CLASS_SID,
       min(case when NOTE_ORDER = 1 then to_char(DESCRLONG) else '' end) NOTE1, 
       min(case when NOTE_ORDER = 2 then to_char(DESCRLONG) else '' end) NOTE2, 
       min(case when NOTE_ORDER = 3 then to_char(DESCRLONG) else '' end) NOTE3, 
       min(case when NOTE_ORDER = 4 then to_char(DESCRLONG) else '' end) NOTE4, 
       min(case when NOTE_ORDER = 5 then to_char(DESCRLONG) else '' end) NOTE5
  from N1 
    GROUP BY CLASS_SID;
