/* ************************************************************ */
/* name: CREATE_SPATIAL_INDEXES.sql                             */
/* purpose: Necessary for spatial qualification to work         */
/* revised: 20091016 lhf, create                                */
/*          20100603 lhf, delete rows and drop indexes first    */
/*          20100706 lhf, add index for SUBSCRIPTION table      */
/*          20120105 dcp, updated SRID to 8307 (WGS 84 geoid)   */
/*          20150728 teh: Adding Peter's sdo_dml_batch_size fix */
/*          20160318 pgm: Add partitioning / remove batch_size  */
/* ************************************************************ */
delete from USER_SDO_GEOM_METADATA;
drop index FM_SPATIAL;
drop index FM_GAZETTEER;
drop index SUB_SPATIAL;

insert INTO USER_SDO_GEOM_METADATA
 values ('FILEMETADATA','FILESPATIALAREA',
  MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',-180,180,.005),
   MDSYS.SDO_DIM_ELEMENT('Y',-90,90,.005)),8307);

insert INTO USER_SDO_GEOM_METADATA
 values ('GAZETTEER','GZLOCATIONSPATIAL',
  MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',-180,180,.005),
   MDSYS.SDO_DIM_ELEMENT('Y',-90,90,.005)),8307);

insert INTO USER_SDO_GEOM_METADATA
 values ('SUBSCRIPTION','SUBSPATIALAREA',
  MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',-180,180,.005),
   MDSYS.SDO_DIM_ELEMENT('Y',-90,90,.005)),8307);


create INDEX FM_GAZETTEER on GAZETTEER(GZLOCATIONSPATIAL) INDEXTYPE is MDSYS.SPATIAL_INDEX;
create INDEX SUB_SPATIAL on SUBSCRIPTION(SUBSPATIALAREA) INDEXTYPE is MDSYS.SPATIAL_INDEX;

create index FM_SPATIAL on filemetadata (filespatialarea)
 indextype is mdsys.spatial_index
local (
partition p1,
partition p2,
partition p3,
partition p4,
partition p5,
partition p6,
partition p7,
partition p8,
partition p9,
partition p10,
partition p11,
partition p12,
partition p13,
partition p14,
partition p15,
partition p16,
partition p17,
partition p18,
partition p19,
partition p20,
partition p21,
partition p22,
partition p23,
partition p24,
partition p25,
partition p26,
partition p27,
partition p28,
partition p29,
partition p30,
partition p31,
partition p32,
partition p33,
partition p34,
partition p35,
partition p36,
partition p37,
partition p38,
partition p39,
partition p40,
partition p41,
partition p42,
partition p43,
partition p44,
partition p45,
partition p46,
partition p47,
partition p48,
partition p49,
partition p50,
partition p51,
partition p52,
partition p53,
partition p54,
partition p55,
partition p56,
partition p57,
partition p58,
partition p59,
partition p60,
partition p61,
partition p62,
partition p63,
partition p64,
partition p65,
partition p66,
partition p67,
partition p68,
partition p69,
partition p70,
partition p71,
partition p72,
partition p73,
partition p74,
partition p75,
partition p76,
partition p77,
partition p78,
partition p79,
partition p80,
partition p81,
partition p82,
partition p83,
partition p84,
partition p85,
partition p86,
partition p87,
partition p88,
partition p89,
partition p90,
partition p91,
partition p92,
partition p93,
partition p94,
partition p95,
partition p96,
partition p97,
partition p98,
partition p99,
partition p100,
partition p101,
partition p102,
partition p103,
partition p104,
partition p105,
partition p106,
partition p107,
partition p108,
partition p109,
partition p110,
partition p111,
partition p112,
partition p113,
partition p114,
partition p115,
partition p116,
partition p117,
partition p118,
partition p119,
partition p120,
partition p121,
partition p122,
partition p123,
partition p124,
partition p125,
partition p126,
partition p127,
partition p128,
partition p129,
partition p130,
partition p131,
partition p132,
partition p133,
partition p134,
partition p135,
partition p136,
partition p137,
partition p138,
partition p139,
partition p140,
partition p141,
partition p142,
partition p143,
partition p144,
partition p145,
partition p146,
partition p147,
partition p148,
partition p149,
partition p150,
partition p151,
partition p152,
partition p153,
partition p154,
partition p155,
partition p156,
partition p157,
partition p158,
partition p159,
partition p160,
partition p161,
partition p162,
partition p163,
partition p164,
partition p165,
partition p166,
partition p167,
partition p168,
partition p169,
partition p170,
partition p171,
partition p172,
partition p173,
partition p174,
partition p175,
partition p176,
partition p177,
partition p178,
partition p179,
partition p180,
partition p181,
partition p182,
partition p183,
partition p184,
partition p185,
partition p186,
partition p187,
partition p188,
partition p189,
partition p190,
partition p191,
partition p192,
partition p193,
partition p194,
partition p195,
partition p196,
partition p197,
partition p198,
partition p199,
partition p9999
)
unusable;

alter index fm_spatial rebuild partition p1;
alter index fm_spatial rebuild partition p2;
alter index fm_spatial rebuild partition p3;
alter index fm_spatial rebuild partition p4;
alter index fm_spatial rebuild partition p5;
alter index fm_spatial rebuild partition p6;
alter index fm_spatial rebuild partition p7;
alter index fm_spatial rebuild partition p8;
alter index fm_spatial rebuild partition p9;
alter index fm_spatial rebuild partition p10;
alter index fm_spatial rebuild partition p11;
alter index fm_spatial rebuild partition p12;
alter index fm_spatial rebuild partition p13;
alter index fm_spatial rebuild partition p14;
alter index fm_spatial rebuild partition p15;
alter index fm_spatial rebuild partition p16;
alter index fm_spatial rebuild partition p17;
alter index fm_spatial rebuild partition p18;
alter index fm_spatial rebuild partition p19;
alter index fm_spatial rebuild partition p20;
alter index fm_spatial rebuild partition p21;
alter index fm_spatial rebuild partition p22;
alter index fm_spatial rebuild partition p23;
alter index fm_spatial rebuild partition p24;
alter index fm_spatial rebuild partition p25;
alter index fm_spatial rebuild partition p26;
alter index fm_spatial rebuild partition p27;
alter index fm_spatial rebuild partition p28;
alter index fm_spatial rebuild partition p29;
alter index fm_spatial rebuild partition p30;
alter index fm_spatial rebuild partition p31;
alter index fm_spatial rebuild partition p32;
alter index fm_spatial rebuild partition p33;
alter index fm_spatial rebuild partition p34;
alter index fm_spatial rebuild partition p35;
alter index fm_spatial rebuild partition p36;
alter index fm_spatial rebuild partition p37;
alter index fm_spatial rebuild partition p38;
alter index fm_spatial rebuild partition p39;
alter index fm_spatial rebuild partition p40;
alter index fm_spatial rebuild partition p41;
alter index fm_spatial rebuild partition p42;
alter index fm_spatial rebuild partition p43;
alter index fm_spatial rebuild partition p44;
alter index fm_spatial rebuild partition p45;
alter index fm_spatial rebuild partition p46;
alter index fm_spatial rebuild partition p47;
alter index fm_spatial rebuild partition p48;
alter index fm_spatial rebuild partition p49;
alter index fm_spatial rebuild partition p50;
alter index fm_spatial rebuild partition p51;
alter index fm_spatial rebuild partition p52;
alter index fm_spatial rebuild partition p53;
alter index fm_spatial rebuild partition p54;
alter index fm_spatial rebuild partition p55;
alter index fm_spatial rebuild partition p56;
alter index fm_spatial rebuild partition p57;
alter index fm_spatial rebuild partition p58;
alter index fm_spatial rebuild partition p59;
alter index fm_spatial rebuild partition p60;
alter index fm_spatial rebuild partition p61;
alter index fm_spatial rebuild partition p62;
alter index fm_spatial rebuild partition p63;
alter index fm_spatial rebuild partition p64;
alter index fm_spatial rebuild partition p65;
alter index fm_spatial rebuild partition p66;
alter index fm_spatial rebuild partition p67;
alter index fm_spatial rebuild partition p68;
alter index fm_spatial rebuild partition p69;
alter index fm_spatial rebuild partition p70;
alter index fm_spatial rebuild partition p71;
alter index fm_spatial rebuild partition p72;
alter index fm_spatial rebuild partition p73;
alter index fm_spatial rebuild partition p74;
alter index fm_spatial rebuild partition p75;
alter index fm_spatial rebuild partition p76;
alter index fm_spatial rebuild partition p77;
alter index fm_spatial rebuild partition p78;
alter index fm_spatial rebuild partition p79;
alter index fm_spatial rebuild partition p80;
alter index fm_spatial rebuild partition p81;
alter index fm_spatial rebuild partition p82;
alter index fm_spatial rebuild partition p83;
alter index fm_spatial rebuild partition p84;
alter index fm_spatial rebuild partition p85;
alter index fm_spatial rebuild partition p86;
alter index fm_spatial rebuild partition p87;
alter index fm_spatial rebuild partition p88;
alter index fm_spatial rebuild partition p89;
alter index fm_spatial rebuild partition p90;
alter index fm_spatial rebuild partition p91;
alter index fm_spatial rebuild partition p92;
alter index fm_spatial rebuild partition p93;
alter index fm_spatial rebuild partition p94;
alter index fm_spatial rebuild partition p95;
alter index fm_spatial rebuild partition p96;
alter index fm_spatial rebuild partition p97;
alter index fm_spatial rebuild partition p98;
alter index fm_spatial rebuild partition p99;
alter index fm_spatial rebuild partition p100;
alter index fm_spatial rebuild partition p101;
alter index fm_spatial rebuild partition p102;
alter index fm_spatial rebuild partition p103;
alter index fm_spatial rebuild partition p104;
alter index fm_spatial rebuild partition p105;
alter index fm_spatial rebuild partition p106;
alter index fm_spatial rebuild partition p107;
alter index fm_spatial rebuild partition p108;
alter index fm_spatial rebuild partition p109;
alter index fm_spatial rebuild partition p110;
alter index fm_spatial rebuild partition p111;
alter index fm_spatial rebuild partition p112;
alter index fm_spatial rebuild partition p113;
alter index fm_spatial rebuild partition p114;
alter index fm_spatial rebuild partition p115;
alter index fm_spatial rebuild partition p116;
alter index fm_spatial rebuild partition p117;
alter index fm_spatial rebuild partition p118;
alter index fm_spatial rebuild partition p119;
alter index fm_spatial rebuild partition p120;
alter index fm_spatial rebuild partition p121;
alter index fm_spatial rebuild partition p122;
alter index fm_spatial rebuild partition p123;
alter index fm_spatial rebuild partition p124;
alter index fm_spatial rebuild partition p125;
alter index fm_spatial rebuild partition p126;
alter index fm_spatial rebuild partition p127;
alter index fm_spatial rebuild partition p128;
alter index fm_spatial rebuild partition p129;
alter index fm_spatial rebuild partition p130;
alter index fm_spatial rebuild partition p131;
alter index fm_spatial rebuild partition p132;
alter index fm_spatial rebuild partition p133;
alter index fm_spatial rebuild partition p134;
alter index fm_spatial rebuild partition p135;
alter index fm_spatial rebuild partition p136;
alter index fm_spatial rebuild partition p137;
alter index fm_spatial rebuild partition p138;
alter index fm_spatial rebuild partition p139;
alter index fm_spatial rebuild partition p140;
alter index fm_spatial rebuild partition p141;
alter index fm_spatial rebuild partition p142;
alter index fm_spatial rebuild partition p143;
alter index fm_spatial rebuild partition p144;
alter index fm_spatial rebuild partition p145;
alter index fm_spatial rebuild partition p146;
alter index fm_spatial rebuild partition p147;
alter index fm_spatial rebuild partition p148;
alter index fm_spatial rebuild partition p149;
alter index fm_spatial rebuild partition p150;
alter index fm_spatial rebuild partition p151;
alter index fm_spatial rebuild partition p152;
alter index fm_spatial rebuild partition p153;
alter index fm_spatial rebuild partition p154;
alter index fm_spatial rebuild partition p155;
alter index fm_spatial rebuild partition p156;
alter index fm_spatial rebuild partition p157;
alter index fm_spatial rebuild partition p158;
alter index fm_spatial rebuild partition p159;
alter index fm_spatial rebuild partition p160;
alter index fm_spatial rebuild partition p161;
alter index fm_spatial rebuild partition p162;
alter index fm_spatial rebuild partition p163;
alter index fm_spatial rebuild partition p164;
alter index fm_spatial rebuild partition p165;
alter index fm_spatial rebuild partition p166;
alter index fm_spatial rebuild partition p167;
alter index fm_spatial rebuild partition p168;
alter index fm_spatial rebuild partition p169;
alter index fm_spatial rebuild partition p170;
alter index fm_spatial rebuild partition p171;
alter index fm_spatial rebuild partition p172;
alter index fm_spatial rebuild partition p173;
alter index fm_spatial rebuild partition p174;
alter index fm_spatial rebuild partition p175;
alter index fm_spatial rebuild partition p176;
alter index fm_spatial rebuild partition p177;
alter index fm_spatial rebuild partition p178;
alter index fm_spatial rebuild partition p179;
alter index fm_spatial rebuild partition p180;
alter index fm_spatial rebuild partition p181;
alter index fm_spatial rebuild partition p182;
alter index fm_spatial rebuild partition p183;
alter index fm_spatial rebuild partition p184;
alter index fm_spatial rebuild partition p185;
alter index fm_spatial rebuild partition p186;
alter index fm_spatial rebuild partition p187;
alter index fm_spatial rebuild partition p188;
alter index fm_spatial rebuild partition p189;
alter index fm_spatial rebuild partition p190;
alter index fm_spatial rebuild partition p191;
alter index fm_spatial rebuild partition p192;
alter index fm_spatial rebuild partition p193;
alter index fm_spatial rebuild partition p194;
alter index fm_spatial rebuild partition p195;
alter index fm_spatial rebuild partition p196;
alter index fm_spatial rebuild partition p197;
alter index fm_spatial rebuild partition p198;
alter index fm_spatial rebuild partition p199;
alter index fm_spatial rebuild partition p9999;

quit
