#!/bin/bash

#
# Overarching Polycom process to be run daily
#

INC_DATE=""
SUFFIX=""

function CHECK_ERROR() {
	if [[ "$1" != "0" ]]
	then
		local ERR="$1"
		shift

		echo "ERROR $ERR during '$@'... Exiting function at $(date)"
		exit 1
	fi
}

function usage() {
	echo "Usage: $0 -d <processing date in yyyy-mm-dd format> [-s <Environment Suffix (UAT|PROD)>]" 1>&2
	exit 1
}

####################################################################################
# Get the command line arg
####################################################################################
while getopts ":s:d:" arg; do
	case "${arg}" in
		s) SUFFIX=${OPTARG} ;;
		d) INC_DATE=${OPTARG} ;;
		*) usage ;;
	esac
done
shift $((OPTIND-1))

if [[ ! $INC_DATE =~ [2][0][0-9][0-9]-[0-1][0-9]-[0-3][0-9] ]]
then
	echo "Date in wrong format $INC_DATE.  Expecting yyyy-mm-dd format."
	usage
fi

DATE_SUFFIX="${INC_DATE//-/}"
DIR_PREFIX="$DATE_SUFFIX"

if [[ ! -z "$SUFFIX" ]]
then
	DATE_SUFFIX="${DATE_SUFFIX}_${SUFFIX}"
fi

#
# DEFINE vars
#
SCRUB_DIR="/data/workspace/polycom/extracts/${DIR_PREFIX}_PolycomExtracts"
LOG_DIR="/data/software/Implementations/data_services/data/prd/logs"
SCRIPT_HOME="/data/software/Implementations/data_services/bookings/tenants/polycom/sh"
DAT_FILE_DIR="/data/workspace/polycom/clientdata"
SQL_HOME="/data/software/Implementations/data_services/bookings/tenants/polycom/sql"
JS_SCRIPT_HOME="/data/software/Implementations/data_services/scripts/api_scripts/common/js"
TENANT="polycom"
RENEW_USER=""
#RENEW_USER="-u bill.moor@polycom.com"
RENEW_PWD=""
#RENEW_PWD="--password "
RENEW_ENVIRONMENT="uat02dl-int.ssi-cloud.com"

#
# Convenience function to call RenueAPI
#
function callRenewAPI() {
	local SCRIPT="$1"
	local INPUT_FILE="$2"
	local COLLECTION="$3"
	local COLLECTION_ARG=
	local ZEN_MODE=""

	shift 3

	if [[ -f "$SCRUB_DIR/$INPUT_FILE" && -s "$SCRUB_DIR/$INPUT_FILE" ]]
	then
		if [[ ! -z "$COLLECTION" ]]
		then
			COLLECTION_ARG="-s $COLLECTION -o update"
		fi

		if [[ $SCRIPT == *resolveAsSuccess* ]] ## || $SCRIPT == *resolveAsLoss* ]]
		then
			ZEN_MODE="--zenMode true"
		fi

		echo "$(date) === Renue API \"$JS_SCRIPT_HOME/$SCRIPT -t $TENANT -h $RENEW_ENVIRONMENT -f $SCRUB_DIR/$INPUT_FILE $COLLECTION_ARG\" $1 $2 $3 $4 $5"
		node $JS_SCRIPT_HOME/$SCRIPT $ZEN_MODE -t $TENANT $RENEW_USER $RENEW_PWD -h $RENEW_ENVIRONMENT -f $SCRUB_DIR/$INPUT_FILE $COLLECTION_ARG $1 $2 $3 $4 $5 &> ${LOG_DIR}/${INPUT_FILE//.csv/.log}
		CHECK_ERROR "$?" "$(date) Error during Renue API call with $INPUT_FILE"
		echo "$(date) Renue API complete."

	else
		echo "$(date) === Empty file OR file \"$SCRUB_DIR/$INPUT_FILE\" does not exist."
	fi
}
#
# Time to call Renew scrubs and upload data
#
# SEQUENCE
# 1. "update_offers_with_values_in_entitlement"	(First step)
# 2. "update_excluded_offers"			(Just before resolve win/loss)
#
# multiAttributes.js		(Yes)	(CALC_OPP, EXCLUDE_OFFER)
# splitOpportunities.js		(Yes)
# resolveAsSuccess.js		(Yes)
# resolveAsLoss.js		(Yes)
# generateOpportunities.js	(Not Used)
# completeOrCancelBookings.js	(Not Used)
# 
#
#	Weekly Disassociated
#
echo "$(date) === Calling RenueAPI for Weekly Disassociated Scrub files"
callRenewAPI "quoteDelivered.js" \
	"polycom_disassociate_resolve_as_loss_reopen_opportunity_${DATE_SUFFIX}.csv"
callRenewAPI "splitOpportunities.js" \
	"polycom_disassociate_split_opportunity_unmatched_offers_${DATE_SUFFIX}.csv"
callRenewAPI "multiAttributes.js" \
	"polycom_disassociate_resolve_as_loss_calculate_opportunity_partial_unmatched_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "multiAttributes.js" \
	"polycom_disassociate_resolve_as_loss_full_match_update_excluded_offers_${DATE_SUFFIX}.csv" \
	"app.offers"
callRenewAPI "multiAttributes.js" \
	"polycom_disassociate_resolve_as_loss_partial_match_update_excluded_offers_${DATE_SUFFIX}.csv" \
	"app.offers"
callRenewAPI "multiAttributes.js" \
	"polycom_disassociate_resolve_as_loss_reset_back_reopened_opportunities_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "multiAttributes.js" \
	"polycom_disassociate_resolve_as_loss_calculate_opportunity_partial_match_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "resolveAsLoss.js" \
	"polycom_disassociate_resolve_as_loss_partial_match_${DATE_SUFFIX}.csv"
callRenewAPI "resolveAsLoss.js" \
	"polycom_disassociate_resolve_as_loss_full_match_${DATE_SUFFIX}.csv"

#
#	Weekly Uninstalled
#
echo "$(date) === Calling RenueAPI for Weekly Uninstalled Scrub files"
callRenewAPI "quoteDelivered.js" \
	"polycom_uninstall_resolve_as_loss_reopen_opportunity_${DATE_SUFFIX}.csv"
callRenewAPI "splitOpportunities.js" \
	"polycom_uninstall_split_opportunity_unmatched_offers_${DATE_SUFFIX}.csv"
callRenewAPI "multiAttributes.js" \
	"polycom_uninstall_resolve_as_loss_calculate_opportunity_partial_unmatched_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "multiAttributes.js" \
	"polycom_uninstall_resolve_as_loss_full_match_update_excluded_offers_${DATE_SUFFIX}.csv" \
	"app.offers"
callRenewAPI "multiAttributes.js" \
	"polycom_uninstall_resolve_as_loss_partial_match_update_excluded_offers_${DATE_SUFFIX}.csv" \
	"app.offers"
callRenewAPI "multiAttributes.js" \
	"polycom_uninstall_resolve_as_loss_reset_back_reopened_opportunities_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "multiAttributes.js" \
	"polycom_uninstall_resolve_as_loss_calculate_opportunity_partial_match_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "resolveAsLoss.js" \
	"polycom_uninstall_resolve_as_loss_partial_match_${DATE_SUFFIX}.csv"
callRenewAPI "resolveAsLoss.js" \
	"polycom_uninstall_resolve_as_loss_full_match_${DATE_SUFFIX}.csv"

#
#	Daily Entitlements
#
echo "$(date) === Calling RenueAPI for Daily ENTITLEMENT Scrub files"
callRenewAPI "quoteDelivered.js" \
	"polycom_entitlement_resolve_as_win_reopen_opportunity_${DATE_SUFFIX}.csv"
callRenewAPI "splitOpportunities.js" \
	"polycom_entitlement_split_opportunity_with_unmatched_offers_${DATE_SUFFIX}.csv"
callRenewAPI "multiAttributes.js" \
	"polycom_entitlement_recalculate_split_opportunity_with_unmatched_offers_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "multiAttributes.js" \
	"polycom_entitlement_resolve_as_win_full_match_update_excluded_offers_${DATE_SUFFIX}.csv" \
	"app.offers"
callRenewAPI "multiAttributes.js" \
	"polycom_entitlement_update_offers_with_values_in_entitlement_${DATE_SUFFIX}.csv" \
	"app.offers"
callRenewAPI "manageRelations.js" \
	"polycom_entitlement_update_offers_product_relationship_${DATE_SUFFIX}.csv" \
	"app.offers" \
	"-d" "app.products" "-r" "product"
callRenewAPI "multiAttributes.js" \
	"polycom_entitlement_resolve_as_win_reset_back_reopened_opportunities_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "multiAttributes.js" \
	"polycom_entitlement_resolve_as_win_update_opportunity_fields_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "multiAttributes.js" \
	"polycom_entitlement_resolve_as_win_reactivated_opportunities_update_booking_date_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "multiAttributes.js" \
	"polycom_entitlement_recalculate_split_opportunity_with_matched_offers_resolve_as_win_${DATE_SUFFIX}.csv" \
	"app.opportunities"
callRenewAPI "resolveAsSuccess.js" \
	"polycom_entitlement_resolve_as_win_reactivated_opportunities_${DATE_SUFFIX}.csv"
callRenewAPI "resolveAsSuccess.js" \
	"polycom_entitlement_resolve_as_win_${DATE_SUFFIX}.csv"

echo "$(date) === Processing complete for \"$INC_DATE\" argument"
