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
* \file   ch-ir-indexer-pvt.c
*
* \author sandeepprakash
*
* \date   Mar 11, 2017
*
* \brief
*
******************************************************************************/

/********************************** INCLUDES **********************************/
#include <ch-pal/exp_pal.h>
#include <ch-utils/exp_list.h>
#include <ch-utils/exp_hashmap.h>
#include <ch-utils/exp_sock_utils.h>
#include <ch-utils/exp_q.h>
#include <ch-utils/exp_msgq.h>
#include <ch-utils/exp_task.h>
#include <ch-sockmon/exp_sockmon.h>
#include "ch-ir-common.h"
#include "ch-ir-indexing-server.h"

static void *ch_ir_indexing_server_task(
	void *p_thread_args);

static SOCKMON_RET_E fn_sockmon_active_sock_cbk(
	SOCKMON_SOCK_ACTIVITY_STATUS_E e_status,
	PAL_SOCK_HDL hl_sock_hdl,
	void *p_app_data);

CH_IR_RET_E ch_ir_indexing_server_init(
	INDEXING_SERVER_INIT_X *px_init_params,
	INDEXING_SERVER_X **ppx_indexing_server_ctxt)
{
	INDEXING_SERVER_X *px_indexing_server = NULL;
	TASK_RET_E e_task_ret = eTASK_RET_FAILURE;
	TASK_CREATE_PARAM_X x_task_param = { { 0 } };

	px_indexing_server = (INDEXING_SERVER_X *)pal_malloc(sizeof(INDEXING_SERVER_X), NULL);

	pal_memmove(&(px_indexing_server->x_init), px_init_params,
		sizeof(px_indexing_server->x_init));

	x_task_param.b_msgq_needed = true;
	x_task_param.ui_msgq_size = 1000;
	x_task_param.fn_task_routine = ch_ir_indexing_server_task;
	x_task_param.p_app_data = px_indexing_server;
	e_task_ret = task_create(&(px_indexing_server->hl_task_hdl),
		&x_task_param);
	if ((eTASK_RET_SUCCESS != e_task_ret)
		|| (NULL == px_indexing_server->hl_task_hdl))
	{
		CH_IR_LOG_LOW("task_create failed: %d, %p", e_task_ret,
			px_indexing_server, px_indexing_server->hl_task_hdl);
		return eCH_IR_RET_RESOURCE_FAILURE;
	}
	else
	{
		e_task_ret = task_start(px_indexing_server->hl_task_hdl);
		if (eTASK_RET_SUCCESS != e_task_ret)
		{
			CH_IR_LOG_LOW("task_start failed: %d", e_task_ret);
			e_task_ret = task_delete(px_indexing_server->hl_task_hdl);
			if (eTASK_RET_SUCCESS != e_task_ret)
			{
				CH_IR_LOG_LOW("task_delete failed: %d", e_task_ret);
			}
			px_indexing_server->hl_task_hdl = NULL;
			return eCH_IR_RET_RESOURCE_FAILURE;
		}
		else
		{
			CH_IR_LOG_MED("task_create success for hl_listner_task_hdl");
			CH_IR_LOG_MED("Indexing server Init Success");
		}
	}

	*ppx_indexing_server_ctxt = px_indexing_server;
	return eCH_IR_RET_SUCCESS;
}

static SOCKMON_RET_E fn_sockmon_active_sock_cbk (
	SOCKMON_SOCK_ACTIVITY_STATUS_E e_status,
	PAL_SOCK_HDL hl_sock_hdl,
	void *p_app_data)
{
	TASK_RET_E e_task_ret = eTASK_RET_FAILURE;
	MSGQ_DATA_X x_data = { NULL };
	IS_NODE_SOCK_ACT_DATA_X *px_sock_act_data = NULL;

	INDEXING_SERVER_X *px_indexing_server = (INDEXING_SERVER_X *) p_app_data;

	CH_IR_LOG_MED("New connection: %d", e_status);

	px_sock_act_data = pal_malloc(sizeof(IS_NODE_SOCK_ACT_DATA_X),
		NULL);

	px_sock_act_data->x_hdr.ui_msg_id = eIS_MSG_ID_LISTENER_SOCK_ACT;
	px_sock_act_data->x_hdr.ui_msg_pay_len = sizeof(*px_sock_act_data) -
		sizeof(px_sock_act_data->x_hdr);
	px_sock_act_data->hl_sock_hdl = hl_sock_hdl;

	x_data.p_data = (uint8_t *) px_sock_act_data;
	x_data.ui_data_size = sizeof (*px_sock_act_data);
	e_task_ret = task_add_msg_to_q(px_indexing_server->hl_task_hdl,
		&x_data, 1000);
	if (eTASK_RET_SUCCESS != e_task_ret)
	{
		CH_IR_LOG_LOW("task_add_msg_to_q failed: %d", e_task_ret);
		return eSOCKMON_RET_RESOURCE_FAILURE;
	}
	else
	{
		return eSOCKMON_RET_SUCCESS;
	}
}

static CH_IR_RET_E ch_ir_indexing_server_handle_sock_act(
	INDEXING_SERVER_X *px_indexing_server,
	IS_MSG_HDR_X *px_msg_hdr)
{
	CH_IR_RET_E e_error = eCH_IR_RET_FAILURE;
	IS_NODE_SOCK_ACT_DATA_X *px_sock_act_data = NULL;
	PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;
	PAL_SOCK_ADDR_IN_X x_in_addr = { 0 };
	PAL_SOCK_HDL hl_sock_hdl = NULL;

	if ((NULL == px_indexing_server) || (NULL == px_msg_hdr))
	{
		CH_IR_LOG_LOW("Invalid Args");
		e_error = eCH_IR_RET_INVALID_ARGS;
		goto CLEAN_RETURN;
	}

	px_sock_act_data = (IS_NODE_SOCK_ACT_DATA_X *)px_msg_hdr;
	CH_IR_LOG_MED("Handling new connection...");

	CH_IR_LOG_MED("New connection. Accepting it.");
	e_pal_ret = pal_sock_accept(px_indexing_server->hl_listen_hdl,
		&x_in_addr, &hl_sock_hdl);
	if ((ePAL_RET_SUCCESS != e_pal_ret) || (NULL == hl_sock_hdl))
	{
		CH_IR_LOG_MED("pal_sock_accept failed: %d", e_pal_ret);
	}
	else
	{
		CH_IR_LOG_MED("New connection on listen socket.");


	}

	CH_IR_LOG_MED("Re-registering the listen socket.");
	SOCKMON_REGISTER_DATA_X x_register = { NULL };
	x_register.fn_active_sock_cbk = fn_sockmon_active_sock_cbk;
	x_register.p_app_data = px_indexing_server;
	x_register.hl_sock_hdl = px_indexing_server->hl_listen_hdl;
	sockmon_register_sock(px_indexing_server->x_init.hl_sockmon_hdl, &x_register);
	e_error = eCH_IR_RET_SUCCESS;

CLEAN_RETURN:
	return e_error;
}

static CH_IR_RET_E ch_ir_indexing_server_handle_msgs(
	INDEXING_SERVER_X *px_indexing_server,
	IS_MSG_HDR_X *px_msg_hdr)
{
	CH_IR_RET_E e_error = eCH_IR_RET_FAILURE;

	if ((NULL == px_indexing_server) || (NULL == px_msg_hdr))
	{
		CH_IR_LOG_LOW("Invalid Args");
		e_error = eCH_IR_RET_INVALID_ARGS;
		goto CLEAN_RETURN;
	}

	switch (px_msg_hdr->ui_msg_id)
	{
	case eIS_MSG_ID_LISTENER_SOCK_ACT:
	{
		CH_IR_LOG_MED("Got eIS_MSG_ID_LISTENER_SOCK_ACT");
		e_error = ch_ir_indexing_server_handle_sock_act(px_indexing_server,
			px_msg_hdr);
		if (eCH_IR_RET_SUCCESS != e_error)
		{
			CH_IR_LOG_LOW("ch_ir_indexing_server_handle_sock_act failed: %d", e_error);
		}
		break;
	}
	default:
	{
		CH_IR_LOG_LOW("Invalid Msg Id: %d", px_msg_hdr->ui_msg_id);
		e_error = eCH_IR_RET_INVALID_ARGS;
		break;
	}
	}

CLEAN_RETURN:
	return e_error;
}

static void *ch_ir_indexing_server_task(
	void *p_thread_args)
{
	PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;
	TASK_RET_E e_task_ret = eTASK_RET_FAILURE;
	CH_IR_RET_E e_error = eCH_IR_RET_FAILURE;
	INDEXING_SERVER_X *px_indexing_server = NULL;
	MSGQ_DATA_X x_data = { NULL };
	IS_MSG_HDR_X *px_msg_hdr = NULL;

	if (NULL == p_thread_args)
	{
		CH_IR_LOG_LOW("Invalid Args");
		goto CLEAN_RETURN;
	}

	px_indexing_server = (INDEXING_SERVER_X *) p_thread_args;

	e_pal_ret = tcp_listen_sock_create(
		&(px_indexing_server->hl_listen_hdl),
		px_indexing_server->x_init.us_listen_port_ho);
	if (ePAL_RET_SUCCESS != e_pal_ret) {
		CH_IR_LOG_HIGH("tcp_listen_sock_create failed: %d", e_pal_ret);
		return NULL;
	}

	CH_IR_LOG_HIGH("tcp_listen_sock_create success. Listening on port: %d",
		px_indexing_server->x_init.us_listen_port_ho);

	SOCKMON_REGISTER_DATA_X x_register = {NULL};
	x_register.fn_active_sock_cbk = fn_sockmon_active_sock_cbk;
	x_register.p_app_data = px_indexing_server;
	x_register.hl_sock_hdl = px_indexing_server->hl_listen_hdl;
	sockmon_register_sock(px_indexing_server->x_init.hl_sockmon_hdl, &x_register);

	while (task_is_in_loop(px_indexing_server->hl_task_hdl))
	{
		e_task_ret = task_get_msg_from_q(px_indexing_server->hl_task_hdl,
			&x_data, 1000);
		if ((eTASK_RET_SUCCESS == e_task_ret) && (NULL != x_data.p_data)
			&& (0 != x_data.ui_data_size))
		{
			px_msg_hdr = (IS_MSG_HDR_X *)x_data.p_data;

			e_error = ch_ir_indexing_server_handle_msgs(px_indexing_server, px_msg_hdr);
			if (eCH_IR_RET_SUCCESS != e_error)
			{
				CH_IR_LOG_LOW("ch_ir_indexing_server_handle_msgs failed: %d", e_error);
			}
		}


		CH_IR_LOG_FULL("Indexing server Listener Task");
	}


CLEAN_RETURN:
	CH_IR_LOG_MED("Notifying task exit");
	e_task_ret = task_notify_exit(px_indexing_server->hl_task_hdl);
	if (eTASK_RET_SUCCESS != e_task_ret)
	{
		CH_IR_LOG_LOW("task_notify_exit failed: %d", e_task_ret);
	}
	else
	{
		CH_IR_LOG_MED("task_notify_exit success");
	}
	return p_thread_args;
}