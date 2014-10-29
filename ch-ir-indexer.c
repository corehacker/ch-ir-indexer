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
 * \file   ch-ir-indexer.c
 *
 * \author sandeepprakash
 *
 * \date   Mar 11, 2014
 *
 * \brief  
 *
 ******************************************************************************/

/********************************** INCLUDES **********************************/
#include <ch-pal/exp_pal.h>
#include <ch-utils/exp_list.h>
#include <ch-utils/exp_hashmap.h>
#include "ch-ir-common.h"
#include "ch-ir-dir-parser.h"
#include "ch-ir-tokenizer.h"
#include "ch-ir-indexer.h"
#include "ch-ir-indexer-pvt.h"
#include "ch-ir-indexer-serialize.h"
#include "ch-ir-indexer-compress.h"
#include "porter.h"

/********************************* CONSTANTS **********************************/

/*********************************** MACROS ***********************************/

/******************************** ENUMERATIONS ********************************/

/************************* STRUCTURE/UNION DATA TYPES *************************/

/************************ STATIC FUNCTION PROTOTYPES **************************/

/****************************** LOCAL FUNCTIONS *******************************/
CH_IR_RET_E ch_ir_indexer_init(
   CH_IR_INDEXER_INIT_PARAMS_X *px_init_params,
   CH_IR_INDEXER_CTXT_X **ppx_indexer_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;
   CH_IR_TOKENIZER_INIT_PARAMS_X x_tokenizer_init_params = {0};
   CH_IR_DIR_PARSER_INIT_PARAMS_X x_dir_parser_init_params = {0};
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   HM_INIT_PARAMS_X x_hm_init_params = { 0 };

   if ((NULL == px_init_params) || (NULL == ppx_indexer_ctxt))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   if ((0 == px_init_params->ui_max_filepath_len)
      || (0 == px_init_params->ui_max_token_len)
      || (0 == px_init_params->ui_token_hm_table_size)
      || (0 == px_init_params->ui_postings_hm_table_size))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   px_indexer_ctxt = pal_malloc (sizeof(CH_IR_INDEXER_CTXT_X), NULL);
   if (NULL == px_indexer_ctxt)
   {
      CH_IR_LOG_MED("pal_malloc failed");
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   (void) pal_memmove(&(px_indexer_ctxt->x_init_params), px_init_params,
      sizeof(px_indexer_ctxt->x_init_params));

   x_tokenizer_init_params.ui_max_token_len = px_init_params->ui_max_token_len;
   x_tokenizer_init_params.fn_ch_ir_for_each_token_cbk = ch_ir_indexer_for_each_token_cbk;
   x_tokenizer_init_params.p_app_data = px_indexer_ctxt;
   e_ret_val = ch_ir_tokenizer_init(&x_tokenizer_init_params,
      &(px_indexer_ctxt->px_tokenizer_ctxt));
   if (eCH_IR_RET_SUCCESS != e_ret_val)
   {
      CH_IR_LOG_MED("ch_ir_tokenizer_init failed: %d", e_ret_val);
      goto CLEAN_RETURN;
   }

   x_dir_parser_init_params.ui_max_filepath_len = px_init_params->ui_max_filepath_len;
   x_dir_parser_init_params.fn_ch_ir_for_each_file_cbk = ch_ir_indexer_for_each_file_cbk;
   x_dir_parser_init_params.p_app_data = px_indexer_ctxt;
   e_ret_val = ch_ir_dir_parser_init(&x_dir_parser_init_params,
      &(px_indexer_ctxt->px_dir_parser_ctxt));
   if (eCH_IR_RET_SUCCESS != e_ret_val)
   {
      CH_IR_LOG_MED("ch_ir_dir_parser_init failed: %d", e_ret_val);
      goto CLEAN_RETURN;
   }

   if (true == px_init_params->b_ignore_stopwords)
   {
      ch_ir_indexer_create_stopwords_cache_hm (px_indexer_ctxt,
         px_init_params->uca_stopwords_filepath);
   }

   x_hm_init_params.e_hm_key_type = eHM_KEY_TYPE_STRING;
   x_hm_init_params.b_maintain_linked_list = true;
   x_hm_init_params.ui_linked_list_flags |= eHM_LINKED_LIST_FLAGS_BM_UNSORTED;
   x_hm_init_params.ui_hm_table_size = px_init_params->ui_token_hm_table_size;
   e_hm_ret = hm_create (&(px_indexer_ctxt->hl_token_hm), &x_hm_init_params);
   if (eHM_RET_SUCCESS != e_hm_ret)
   {
      CH_IR_LOG_MED("hm_create failed: %d", e_hm_ret);
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }


   *ppx_indexer_ctxt = px_indexer_ctxt;
   e_ret_val = eCH_IR_RET_SUCCESS;
CLEAN_RETURN:
   if (eCH_IR_RET_SUCCESS != e_ret_val)
   {
      (void) ch_ir_indexer_deinit(px_indexer_ctxt);
   }
   return e_ret_val;
}

CH_IR_RET_E ch_ir_indexer_deinit(
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_DATA_STRUCT_INVALID};
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;

   if (NULL == px_indexer_ctxt)
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   (void) ch_ir_indexer_delete_index (px_indexer_ctxt);
   /*
    * TODO: Cleanup stopwords hashmap.
    */

   if (NULL != px_indexer_ctxt->hl_stopword_hm)
   {
      x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
      x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
      e_hm_ret = hm_for_each_v2 (px_indexer_ctxt->hl_token_hm,
         &x_for_each_param, ch_ir_indexer_stopwords_hm_for_each_delete_cbk,
         px_indexer_ctxt);

      (void) hm_delete_all_nodes (px_indexer_ctxt->hl_stopword_hm);

      (void) hm_delete (px_indexer_ctxt->hl_stopword_hm);
      px_indexer_ctxt->hl_stopword_hm = NULL;
   }

   if (NULL != px_indexer_ctxt->px_dir_parser_ctxt)
   {
      (void) ch_ir_dir_parser_deinit(px_indexer_ctxt->px_dir_parser_ctxt);
      px_indexer_ctxt->px_dir_parser_ctxt = NULL;
   }

   if (NULL != px_indexer_ctxt->px_tokenizer_ctxt)
   {
      (void) ch_ir_tokenizer_deinit(px_indexer_ctxt->px_tokenizer_ctxt);
      px_indexer_ctxt->px_tokenizer_ctxt = NULL;
   }

   if (NULL != px_indexer_ctxt->hl_token_hm)
   {
      (void) hm_delete (px_indexer_ctxt->hl_token_hm);
      px_indexer_ctxt->hl_token_hm = NULL;
   }

   pal_free (px_indexer_ctxt);
   px_indexer_ctxt = NULL;
   e_ret_val = eCH_IR_RET_SUCCESS;

CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_indexer_build_index (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   uint8_t *puc_dir_path)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   uint32_t ui_start_ms = 0;
   uint32_t ui_end_ms = 0;

   if ((NULL == px_indexer_ctxt) || (NULL == puc_dir_path))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   ui_start_ms = pal_get_system_time_ms();
   e_ret_val = ch_ir_dir_parser_parse_dir_for_files (
      px_indexer_ctxt->px_dir_parser_ctxt, puc_dir_path);
   if (eCH_IR_RET_SUCCESS != e_ret_val)
   {
      CH_IR_LOG_MED("ch_ir_dir_parser_parse_dir_for_files failed: %d",
         e_ret_val);
      goto CLEAN_RETURN;
   }

   ui_end_ms = pal_get_system_time_ms();
   px_indexer_ctxt->x_stats.ui_tokenization_time_ms = ui_end_ms
      - ui_start_ms;

   ui_start_ms = pal_get_system_time_ms();
   e_ret_val = ch_ir_tokenizer_get_stats (px_indexer_ctxt->px_tokenizer_ctxt,
      &(px_indexer_ctxt->x_stats.x_tokenizer_stats));

   e_hm_ret = hm_get_total_count (px_indexer_ctxt->hl_token_hm,
      &(px_indexer_ctxt->x_stats.ui_num_unique_tokens));
   if (eHM_RET_SUCCESS != e_hm_ret)
   {
      CH_IR_LOG_MED("hm_get_total_count failed: %d", e_hm_ret);
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
      goto CLEAN_RETURN;
   }

   e_hm_ret = hm_linked_list_sort (px_indexer_ctxt->hl_token_hm,
      ch_ir_indexer_token_hm_compare_cbk, px_indexer_ctxt);
   if (eHM_RET_SUCCESS != e_hm_ret)
   {
      CH_IR_LOG_MED("hm_linked_list_sort failed: %d", e_hm_ret);
      e_ret_val = eCH_IR_RET_RESOURCE_FAILURE;
   }
   else
   {
      ui_end_ms = pal_get_system_time_ms();
      px_indexer_ctxt->x_stats.ui_token_sort_time_ms = ui_end_ms
         - ui_start_ms;
      e_ret_val = eCH_IR_RET_SUCCESS;
   }
CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_indexer_print_stats (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   uint8_t *puc_token)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   HM_FOR_EACH_PARAMS_X x_for_each_param = {eHM_DATA_STRUCT_INVALID};
   HM_RET_E e_hm_ret = eHM_RET_FAILURE;
   uint32_t ui_total_time_ms = 0;
   HM_NODE_DATA_X x_node_data = { eHM_KEY_TYPE_INVALID };
   CH_IR_TOKEN_HM_ENTRY_X *px_token_hm_entry = NULL;

   if (NULL == px_indexer_ctxt)
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   if (NULL == puc_token)
   {
#if 0
      printf ("30 most frequent words:\n");
      printf ("|-%7s-+-%30s-+-%10s-+-%7s|\n", "-------",
         "------------------------------", "----------", "---------");
      printf ("| %7s | %30s | %10s | %7s|\n", "Sl. No.", "Token", "Occurances",
         "Frequency");
      printf ("|-%7s-+-%30s-+-%10s-+-%7s|\n", "-------",
         "------------------------------", "----------", "---------");

      x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
      x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
      e_hm_ret = hm_for_each_v2 (px_indexer_ctxt->hl_token_hm,
         &x_for_each_param, ch_ir_indexer_token_hm_for_each_cbk,
         px_indexer_ctxt);

      printf ("|-%7s-+-%30s-+-%10s-+-%7s|\n", "-------",
         "------------------------------", "----------", "---------");
#endif
      printf ("\n");

      printf ("******************Token Stats******************\n");
      printf ("* Total Tokens Ignored           : %10d *\n",
         px_indexer_ctxt->x_stats.ui_num_tokens_ignored);
      printf ("* Total Unique Tokens            : %10d *\n",
         px_indexer_ctxt->x_stats.ui_num_unique_tokens);
      printf ("* Total Tokens                   : %10d *\n",
         px_indexer_ctxt->x_stats.x_tokenizer_stats.ui_token_count);
      printf ("***********************************************\n");

      printf ("\n");

      ui_total_time_ms = px_indexer_ctxt->x_stats.ui_tokenization_time_ms
         + px_indexer_ctxt->x_stats.ui_token_sort_time_ms
         + px_indexer_ctxt->x_stats.ui_index_serialization_time_ms
         + px_indexer_ctxt->x_stats.ui_index_compression_time_ms
         + px_indexer_ctxt->x_stats.ui_compressed_index_serialization_time_ms;

      printf ("*****************Timing Stats******************\n");
      printf ("* Tokenization                   : %7d ms *\n",
         px_indexer_ctxt->x_stats.ui_tokenization_time_ms);
      printf ("* Token Sorting                  : %7d ms *\n",
         px_indexer_ctxt->x_stats.ui_token_sort_time_ms);
      printf ("* Index Serialization            : %7d ms *\n",
         px_indexer_ctxt->x_stats.ui_index_serialization_time_ms);
      printf ("* Index Compression              : %7d ms *\n",
         px_indexer_ctxt->x_stats.ui_index_compression_time_ms);
      printf ("* Compressed Index Serialization : %7d ms *\n",
         px_indexer_ctxt->x_stats.ui_compressed_index_serialization_time_ms);
      printf ("* -------------------------------+----------- *\n");
      printf ("* Total Time To Build Index      : %7d ms *\n",
         ui_total_time_ms);
      printf ("***********************************************\n");

      printf ("\n");

      printf ("******************Index Stats******************\n");
      printf ("* Number of Inverted List       : %8d    *\n",
         px_indexer_ctxt->x_stats.ui_num_unique_tokens);
      printf ("* Uncompressed Index            : %8d KB *\n",
         px_indexer_ctxt->x_stats.ui_uncompressed_index_size_bytes / 1024);
      printf ("* Compressed Index              : %8d KB *\n",
         px_indexer_ctxt->x_stats.ui_compressed_index_size_bytes / 1024);
      printf ("* Compression Ratio             : %8.3f    *\n",
         (float) px_indexer_ctxt->x_stats.ui_uncompressed_index_size_bytes /
         (float) px_indexer_ctxt->x_stats.ui_compressed_index_size_bytes);

      printf ("***********************************************\n");

      printf ("\n");
   }
   else
   {
      puc_token = (uint8_t *) porter_stem ((char *) puc_token);

      (void) pal_memset (&x_node_data, 0x00, sizeof(x_node_data));
      x_node_data.e_hm_key_type = eHM_KEY_TYPE_STRING;
      x_node_data.u_hm_key.puc_str_key = puc_token;
      e_hm_ret = hm_search_node (px_indexer_ctxt->hl_token_hm, &x_node_data);
      if (eHM_RET_HM_NODE_FOUND == e_hm_ret)
      {
         px_token_hm_entry = (CH_IR_TOKEN_HM_ENTRY_X *) x_node_data.p_data;

         printf ("\nToken %s Found: Document Frequency: %d, Total Occurances: %d\n",
            puc_token,
            px_token_hm_entry->x_postings.ui_doc_freq,
            px_token_hm_entry->ui_num_occurances);
         printf ("Interpret the following data as (Doc Id, Term Frequency, Gap)\n");

         x_for_each_param.e_data_structure = eHM_DATA_STRUCT_LINKED_LIST;
         x_for_each_param.e_direction = eHM_FOR_EACH_DIRECTION_FORWARD;
         e_hm_ret = hm_for_each_v2 (px_token_hm_entry->x_postings.hl_posting_hm,
            &x_for_each_param, ch_ir_indexer_postings_hm_for_each_cbk,
            px_indexer_ctxt);

         printf ("\n");
      }
   }
   e_ret_val = eCH_IR_RET_SUCCESS;
CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_indexer_compress_index (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   uint32_t ui_start_ms = 0;
   uint32_t ui_end_ms = 0;

   if (NULL == px_indexer_ctxt)
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   ui_start_ms = pal_get_system_time_ms ();
   e_ret_val = ch_ir_compress_index (px_indexer_ctxt);
   ui_end_ms = pal_get_system_time_ms ();

   px_indexer_ctxt->x_stats.ui_index_compression_time_ms = ui_end_ms
      - ui_start_ms;

CLEAN_RETURN:
   return e_ret_val;
}

CH_IR_RET_E ch_ir_indexer_serialize (
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt,
   CH_IR_INDEX_SERIALIZE_OPTIONS_X *px_options)
{
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   uint32_t ui_start_ms = 0;
   uint32_t ui_end_ms = 0;

   if ((NULL == px_indexer_ctxt) || (NULL == px_options))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   if (((true == px_options->b_serialize_compressed)
      && (NULL == px_options->puc_compressed_filepath))
      || ((true == px_options->b_serialize_uncompressed)
         && (NULL == px_options->puc_uncompressed_filepath)))
   {
      CH_IR_LOG_MED("Invalid Args");
      e_ret_val = eCH_IR_RET_INVALID_ARGS;
      goto CLEAN_RETURN;
   }

   if (true == px_options->b_serialize_uncompressed)
   {
      ui_start_ms = pal_get_system_time_ms ();
      e_ret_val = ch_ir_indexer_serialize_uncompressed_index (px_indexer_ctxt,
         px_options->puc_uncompressed_filepath);
      ui_end_ms = pal_get_system_time_ms ();

      px_indexer_ctxt->x_stats.ui_index_serialization_time_ms = ui_end_ms
         - ui_start_ms;

   }
   else
   {
      e_ret_val = eCH_IR_RET_SUCCESS;
   }

   if (true == px_options->b_serialize_compressed)
   {
      ui_start_ms = pal_get_system_time_ms ();
      e_ret_val = ch_ir_indexer_serialize_compressed_index (px_indexer_ctxt,
         px_options->puc_compressed_filepath);
      ui_end_ms = pal_get_system_time_ms ();

      px_indexer_ctxt->x_stats.ui_compressed_index_serialization_time_ms =
         ui_end_ms - ui_start_ms;
   }
   else
   {
      e_ret_val = eCH_IR_RET_SUCCESS;
   }
CLEAN_RETURN:
   return e_ret_val;
}
