/*
Navicat MySQL Data Transfer

Source Server         : 47.93.56.72_3306
Source Server Version : 50173
Source Host           : 47.93.56.72:3306
Source Database       : test

Target Server Type    : MYSQL
Target Server Version : 50173
File Encoding         : 65001

Date: 2017-06-19 22:04:35
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for time_dim
-- ----------------------------
DROP TABLE IF EXISTS `time_dim`;
CREATE TABLE `time_dim` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `time` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=25 DEFAULT CHARSET=latin1;

-- ----------------------------
-- Records of time_dim
-- ----------------------------
INSERT INTO `time_dim` VALUES ('1', '0');
INSERT INTO `time_dim` VALUES ('2', '1');
INSERT INTO `time_dim` VALUES ('3', '2');
INSERT INTO `time_dim` VALUES ('4', '3');
INSERT INTO `time_dim` VALUES ('5', '4');
INSERT INTO `time_dim` VALUES ('6', '5');
INSERT INTO `time_dim` VALUES ('7', '6');
INSERT INTO `time_dim` VALUES ('8', '7');
INSERT INTO `time_dim` VALUES ('9', '8');
INSERT INTO `time_dim` VALUES ('10', '9');
INSERT INTO `time_dim` VALUES ('11', '10');
INSERT INTO `time_dim` VALUES ('12', '11');
INSERT INTO `time_dim` VALUES ('13', '12');
INSERT INTO `time_dim` VALUES ('14', '13');
INSERT INTO `time_dim` VALUES ('15', '14');
INSERT INTO `time_dim` VALUES ('16', '15');
INSERT INTO `time_dim` VALUES ('17', '16');
INSERT INTO `time_dim` VALUES ('18', '17');
INSERT INTO `time_dim` VALUES ('19', '18');
INSERT INTO `time_dim` VALUES ('20', '19');
INSERT INTO `time_dim` VALUES ('21', '20');
INSERT INTO `time_dim` VALUES ('22', '21');
INSERT INTO `time_dim` VALUES ('23', '22');
INSERT INTO `time_dim` VALUES ('24', '23');
