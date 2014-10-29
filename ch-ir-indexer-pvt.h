/*******************************************************************************
 *  Repository for C modules.
 *  Copyright (C) 2014 Sandeep Prakash
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
 * Copyright (c) 2014, Sandeep Prakash <123sandy@gmail.com>
 *
 * \file   ch-ir-indexer-pvt.h
 *
 * \author sandeepprakash
 *
 * \date   Mar 11, 2014
 *
 * \brief  
 *
 ******************************************************************************/

#ifndef __CH_IR_INDEXER_PVT_H__
#define __CH_IR_INDEXER_PVT_H__

#ifdef  __cplusplus
extern  "C"
{
#endif

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/

/******************************** ENUMERATIONS ********************************/

/*********************** CLASS/STRUCTURE/UNION DATA TYPES *********************/

/***************************** FUNCTION PROTOTYPES ****************************/
void ch_ir_indexer_handle_token_found_in_hm (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry,
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx);

void ch_ir_indexer_handle_token_not_found_in_hm (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx);

void ch_ir_indexer_handle_token(
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx);

CH_IR_RET_E ch_ir_indexer_for_each_token_cbk (
   uint8_t *puc_token,
   uint8_t *puc_file_path,
   uint32_t ui_file_idx,
   void *p_app_data);

CH_IR_RET_E ch_ir_indexer_for_each_file_cbk (
   uint8_t *puc_file_path,
   uint32_t ui_file_idx,
   void *p_app_data);

HM_RET_E ch_ir_indexer_token_hm_compare_cbk(
   HM_NODE_DATA_X *px_node_a_data,
   HM_NODE_DATA_X *px_node_b_data,
   void *p_app_data);

HM_RET_E ch_ir_indexer_postings_hm_for_each_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data);

HM_RET_E ch_ir_indexer_token_hm_for_each_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data);

void ch_ir_indexer_create_stopwords_cache_hm (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   uint8_t *puc_stopwords_filepath);

HM_RET_E ch_ir_indexer_postings_hm_for_each_delete_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data);

HM_RET_E ch_ir_indexer_token_hm_for_each_delete_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data);

HM_RET_E ch_ir_indexer_stopwords_hm_for_each_delete_cbk(
   HM_NODE_DATA_X *px_curr_node_data,
   void *p_app_data);

CH_IR_RET_E ch_ir_indexer_delete_index (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt);

#ifdef   __cplusplus
}
#endif

#endif /* __CH_IR_INDEXER_PVT_H__ */
