/*
Navicat MySQL Data Transfer

Source Server         : 47.93.56.72_3306
Source Server Version : 50173
Source Host           : 47.93.56.72:3306
Source Database       : test

Target Server Type    : MYSQL
Target Server Version : 50173
File Encoding         : 65001

Date: 2017-06-19 22:03:50
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for source_dim
-- ----------------------------
DROP TABLE IF EXISTS `source_dim`;
CREATE TABLE `source_dim` (
  `id` text CHARACTER SET utf8,
  `type` text CHARACTER SET utf8,
  `source` text CHARACTER SET utf8,
  `addr` text CHARACTER SET utf8,
  `match` varchar(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- ----------------------------
-- Records of source_dim
-- ----------------------------
INSERT INTO `source_dim` VALUES ('001', '直接访问', '直接访问', 'www.twotiger.com', 'www.twotiger.com');
INSERT INTO `source_dim` VALUES ('002', '超级链接', '超级链接', '超级链接', 'link');
INSERT INTO `source_dim` VALUES ('003', '搜索引擎', '百度搜索', 'www.baidu.com', 'www.baidu.com');
INSERT INTO `source_dim` VALUES ('004', '搜索引擎', '谷歌搜索', 'www.google.com', 'www.google.com');
INSERT INTO `source_dim` VALUES ('005', '搜索引擎', '360搜索', 'www.so.com', 'www.so.com');
INSERT INTO `source_dim` VALUES ('006', '搜索引擎', '雅虎搜索', 'sg.search.yahoo.com', 'sg.search.yahoo.com');
INSERT INTO `source_dim` VALUES ('007', '搜索引擎', '搜狗搜索', 'www.sogou.com', 'www.sogou.com');
INSERT INTO `source_dim` VALUES ('008', '搜索引擎', '有道搜索', 'www.youdao.com', 'www.youdao.com');
INSERT INTO `source_dim` VALUES ('009', '搜索引擎', '必应搜索', 'cn.bing.com', 'cn.bing.com');
INSERT INTO `source_dim` VALUES ('010', '导航网站', 'hao123导航', 'www.hao123.com', 'www.hao123.com');
INSERT INTO `source_dim` VALUES ('011', '导航网站', '360导航', 'hao.360.cn', 'hao.360.cn');
INSERT INTO `source_dim` VALUES ('012', '导航网站', '2345导航', 'www.2345.com', 'www.2345.com');
INSERT INTO `source_dim` VALUES ('013', '导航网站', 'QQ导航', 'hao.qq.com', 'hao.qq.com');
INSERT INTO `source_dim` VALUES ('014', '导航网站', '搜狗导航', '123.sogou.com', '123.sogou.com');
INSERT INTO `source_dim` VALUES ('015', '导航网站', '265导航', 'www.265.com', 'www.265.com');
INSERT INTO `source_dim` VALUES ('016', '导航网站', '114导航', 'www.114la.com', 'www.114la.com');
