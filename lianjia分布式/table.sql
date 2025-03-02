CREATE TABLE `fangwuxinxi` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `addtime` timestamp NOT NULL DEFAULT current_timestamp() COMMENT '创建时间',
  `biaoti` varchar(200) NOT NULL COMMENT '标题',
  `fangwubianhao` varchar(200) DEFAULT NULL COMMENT '房屋编号',
  `fangwuleixing` varchar(200) DEFAULT NULL COMMENT '房屋类型',
  `huxing` varchar(200) DEFAULT NULL COMMENT '户型',
  `zulinfangshi` varchar(200) DEFAULT NULL COMMENT '租赁方式',
  `yuyuekanfang` varchar(200) DEFAULT NULL COMMENT '预约看房',
  `chengshi` varchar(200) DEFAULT NULL COMMENT '城市',
  `dizhi` varchar(200) DEFAULT NULL COMMENT '地址',
  `tupian` varchar(200) DEFAULT NULL COMMENT '图片',
  `mianji` varchar(200) DEFAULT NULL COMMENT '面积',
  `zujin` float DEFAULT NULL COMMENT '租金',
  `fangyuansheshi` longtext DEFAULT NULL COMMENT '房源设施',
  `hexinmaidian` longtext DEFAULT NULL COMMENT '核心卖点',
  `faburiqi` date DEFAULT NULL COMMENT '发布日期',
  `fangyuanxiangqing` longtext DEFAULT NULL COMMENT '房源详情',
  `fangzhuzhanghao` varchar(200) DEFAULT NULL COMMENT '001',
  `fangzhuxingming` varchar(200) DEFAULT NULL COMMENT '房主姓名',
  `lianxidianhua` varchar(200) DEFAULT NULL COMMENT '联系电话',
  `sfsh` varchar(200) DEFAULT '否' COMMENT '是否审核',
  `shhf` longtext DEFAULT NULL COMMENT '审核回复',
  `clicktime` datetime DEFAULT NULL COMMENT '最近点击时间',
  `clicknum` int(11) DEFAULT 0 COMMENT '点击次数',
  PRIMARY KEY (`id`),
  UNIQUE KEY `fangwubianhao` (`fangwubianhao`)
) ENGINE=InnoDB AUTO_INCREMENT=47 DEFAULT CHARSET=utf8 COMMENT='房屋信息';

-- ----------------------------
-- Records of fangwuxinxi
-- ----------------------------
INSERT INTO `fangwuxinxi` VALUES ('41', '2024-07-07 00:33:12', '2室整租', '1111111111', '普通商品房', '户型1', '整租', '接受', '城市1', '地址1', 'upload/fangwuxinxi_tupian1.jpg', '面积1', '1', '房源设施1', '核心卖点1', '2022-04-07', '房源详情1', '0011', '张三', '联系电话1', '是', '', '2022-04-07 00:33:12', '1');
INSERT INTO `fangwuxinxi` VALUES ('42', '2024-07-07 00:33:12', '精品出租', '2222222222', '经济适用房', '户型2', '整租', '接受', '城市2', '地址2', 'upload/fangwuxinxi_tupian2.jpg', '面积2', '2', '房源设施2', '核心卖点2', '2022-04-07', '房源详情2', '0012', '李四', '联系电话2', '是', '', '2022-04-07 00:33:12', '2');
INSERT INTO `fangwuxinxi` VALUES ('43', '2024-07-07 00:33:12', '一室一厅', '3333333333', '小产权房', '户型3', '整租', '接受', '城市3', '地址3', 'upload/fangwuxinxi_tupian3.jpg', '面积3', '3', '房源设施3', '核心卖点3', '2022-04-07', '房源详情3', '0013', '王五', '联系电话3', '是', '', '2022-04-07 00:33:12', '3');
INSERT INTO `fangwuxinxi` VALUES ('44', '2024-07-07 00:33:12', '大三室', '4444444444', '房改房', '户型4', '整租', '接受', '城市4', '地址4', 'upload/fangwuxinxi_tupian4.jpg', '面积4', '4', '房源设施4', '核心卖点4', '2022-04-07', '房源详情4', '0014', '李明', '联系电话4', '是', '', '2022-04-07 00:33:12', '4');
INSERT INTO `fangwuxinxi` VALUES ('45', '2024-07-07 00:33:12', '便宜出租了', '5555555555', '廉租房', '户型5', '整租', '接受', '城市5', '地址5', 'upload/fangwuxinxi_tupian5.jpg', '面积5', '5', '房源设施5', '核心卖点5', '2022-04-07', '房源详情5', '0015', '王强', '联系电话5', '是', '', '2022-04-07 00:33:12', '5');
INSERT INTO `fangwuxinxi` VALUES ('46', '2024-07-07 00:33:12', '长期出去', '6666666666', '公租房', '户型6', '整租', '接受', '城市6', '地址6', 'upload/fangwuxinxi_tupian6.jpg', '面积6', '6', '房源设施6', '核心卖点6', '2022-04-07', '房源详情6', '0016', '张飞', '联系电话6', '是', '', '2022-04-07 00:33:12', '6');