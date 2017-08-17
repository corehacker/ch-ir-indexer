/*******************************************************************************
*  Repository for C modules.
*  Copyright (C) 2017 Sandeep Prakash
*
*  This program is free software; you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation; either version 2 of the License, or
*  (at your option) any later version.
*
*  This program is distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
*  GNU General Public License for more details.
*
*  You should have received a copy of the GNU General Public License along
*  with this program; if not, write to the Free Software Foundation, Inc.,
*  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
******************************************************************************/

/*******************************************************************************
* Copyright (c) 2017, Sandeep Prakash <123sandy@gmail.com>
*
* \file   ch-ir-indexing-server.h
*
* \author sandeepprakash
*
* \date   August 11, 2017
*
* \brief
*
******************************************************************************/

#ifndef __CH_IR_INDEXING_SERVER_H__
#define __CH_IR_INDEXING_SERVER_H__

#ifdef  __cplusplus
extern  "C"
{
#endif

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/

/*********************** CLASS/STRUCTURE/UNION DATA TYPES *********************/
typedef struct _INDEXING_SERVER_INIT_X {
	uint16_t          us_listen_port_ho;

	SOCKMON_HDL hl_sockmon_hdl;
} INDEXING_SERVER_INIT_X;

typedef struct _INDEXING_SERVER_X {
	INDEXING_SERVER_INIT_X x_init;

	PAL_SOCK_HDL hl_listen_hdl;

	TASK_HDL hl_task_hdl;

	HM_HDL hl_peer_hm;
} INDEXING_SERVER_X;

typedef enum _IS_MSG_ID_E
{
	eIS_MSG_ID_INVALID = 0x00000000,

	eIS_MSG_ID_LISTENER_START = 0x00001001,

	eIS_MSG_ID_LISTENER_SOCK_ACT = 0x00001002,
	/*!< #_IS_NODE_SOCK_ACT_DATA_X */

	eIS_MSG_ID_CONN_SOCK_ACT = 0x00001003,
	/*!< #_IS_NODE_SOCK_ACT_DATA_X */

	eIS_MSG_ID_LISTENER_END = 0x00002000,

	eIS_MSG_ID_MAX,
} IS_MSG_ID_E;

typedef struct _IS_MSG_HDR_X
{
	uint32_t ui_msg_id;

	uint32_t ui_msg_pay_len;

	uint8_t uca_reserved[56];
} IS_MSG_HDR_X;

typedef struct _IS_NODE_SOCK_ACT_DATA_X
{
	IS_MSG_HDR_X x_hdr;

	PAL_SOCK_HDL hl_sock_hdl;
} IS_NODE_SOCK_ACT_DATA_X;

/***************************** FUNCTION PROTOTYPES ****************************/

CH_IR_RET_E ch_ir_indexing_server_init(
	INDEXING_SERVER_INIT_X *px_init_params,
	INDEXING_SERVER_X **ppx_tokenizer_ctxt);

#ifdef   __cplusplus
}
#endif

#endif /* __CH_IR_INDEXING_SERVER_H__ */