    const INTERNAL_ERR: &'static str = "`ethabi_derive` internal error";
    /// Contract's events.
    #[allow(dead_code)]
    pub mod events {
        use super::INTERNAL_ERR;
        #[derive(Debug, Clone, PartialEq)]
        pub struct Burn {
            pub owner: Vec<u8>,
            pub tick_lower: num_bigint::BigInt,
            pub tick_upper: num_bigint::BigInt,
            pub amount: ethabi::Uint,
            pub amount0: ethabi::Uint,
            pub amount1: ethabi::Uint,
        }
        impl Burn {
            const TOPIC_ID: [u8; 32] = [
                12u8,
                57u8,
                108u8,
                217u8,
                137u8,
                163u8,
                159u8,
                68u8,
                89u8,
                181u8,
                250u8,
                26u8,
                237u8,
                106u8,
                154u8,
                141u8,
                205u8,
                188u8,
                69u8,
                144u8,
                138u8,
                207u8,
                214u8,
                126u8,
                2u8,
                140u8,
                213u8,
                104u8,
                218u8,
                152u8,
                152u8,
                44u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 4usize {
                    return false;
                }
                if log.data.len() != 96usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                let mut values = ethabi::decode(
                        &[
                            ethabi::ParamType::Uint(128usize),
                            ethabi::ParamType::Uint(256usize),
                            ethabi::ParamType::Uint(256usize),
                        ],
                        log.data.as_ref(),
                    )
                    .map_err(|e| format!("unable to decode log.data: {}", e))?;
                values.reverse();
                Ok(Self {
                    owner: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[1usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'owner' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    tick_lower: num_bigint::BigInt::from_signed_bytes_be(
                        log.topics[2usize].as_ref(),
                    ),
                    tick_upper: num_bigint::BigInt::from_signed_bytes_be(
                        log.topics[3usize].as_ref(),
                    ),
                    amount: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    amount0: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    amount1: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => panic!("Unable to decode logs.Burn event: {:#}", e),
                }
            }
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct Collect {
            pub owner: Vec<u8>,
            pub recipient: Vec<u8>,
            pub tick_lower: num_bigint::BigInt,
            pub tick_upper: num_bigint::BigInt,
            pub amount0: ethabi::Uint,
            pub amount1: ethabi::Uint,
        }
        impl Collect {
            const TOPIC_ID: [u8; 32] = [
                112u8,
                147u8,
                83u8,
                56u8,
                230u8,
                151u8,
                117u8,
                69u8,
                106u8,
                133u8,
                221u8,
                239u8,
                34u8,
                108u8,
                57u8,
                95u8,
                182u8,
                104u8,
                182u8,
                63u8,
                160u8,
                17u8,
                95u8,
                95u8,
                32u8,
                97u8,
                11u8,
                56u8,
                142u8,
                108u8,
                169u8,
                192u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 4usize {
                    return false;
                }
                if log.data.len() != 96usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                let mut values = ethabi::decode(
                        &[
                            ethabi::ParamType::Address,
                            ethabi::ParamType::Uint(128usize),
                            ethabi::ParamType::Uint(128usize),
                        ],
                        log.data.as_ref(),
                    )
                    .map_err(|e| format!("unable to decode log.data: {}", e))?;
                values.reverse();
                Ok(Self {
                    owner: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[1usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'owner' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    tick_lower: num_bigint::BigInt::from_signed_bytes_be(
                        log.topics[2usize].as_ref(),
                    ),
                    tick_upper: num_bigint::BigInt::from_signed_bytes_be(
                        log.topics[3usize].as_ref(),
                    ),
                    recipient: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    amount0: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    amount1: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => panic!("Unable to decode logs.Collect event: {:#}", e),
                }
            }
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct CollectProtocol {
            pub sender: Vec<u8>,
            pub recipient: Vec<u8>,
            pub amount0: ethabi::Uint,
            pub amount1: ethabi::Uint,
        }
        impl CollectProtocol {
            const TOPIC_ID: [u8; 32] = [
                89u8,
                107u8,
                87u8,
                57u8,
                6u8,
                33u8,
                141u8,
                52u8,
                17u8,
                133u8,
                11u8,
                38u8,
                166u8,
                180u8,
                55u8,
                214u8,
                196u8,
                82u8,
                47u8,
                219u8,
                67u8,
                210u8,
                210u8,
                56u8,
                98u8,
                99u8,
                248u8,
                109u8,
                80u8,
                184u8,
                177u8,
                81u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 3usize {
                    return false;
                }
                if log.data.len() != 64usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                let mut values = ethabi::decode(
                        &[
                            ethabi::ParamType::Uint(128usize),
                            ethabi::ParamType::Uint(128usize),
                        ],
                        log.data.as_ref(),
                    )
                    .map_err(|e| format!("unable to decode log.data: {}", e))?;
                values.reverse();
                Ok(Self {
                    sender: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[1usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'sender' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    recipient: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[2usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'recipient' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    amount0: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    amount1: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => {
                        panic!("Unable to decode logs.CollectProtocol event: {:#}", e)
                    }
                }
            }
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct Flash {
            pub sender: Vec<u8>,
            pub recipient: Vec<u8>,
            pub amount0: ethabi::Uint,
            pub amount1: ethabi::Uint,
            pub paid0: ethabi::Uint,
            pub paid1: ethabi::Uint,
        }
        impl Flash {
            const TOPIC_ID: [u8; 32] = [
                189u8,
                189u8,
                183u8,
                29u8,
                120u8,
                96u8,
                55u8,
                107u8,
                165u8,
                43u8,
                37u8,
                165u8,
                2u8,
                139u8,
                238u8,
                162u8,
                53u8,
                129u8,
                54u8,
                74u8,
                64u8,
                82u8,
                47u8,
                107u8,
                207u8,
                184u8,
                107u8,
                177u8,
                242u8,
                220u8,
                166u8,
                51u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 3usize {
                    return false;
                }
                if log.data.len() != 128usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                let mut values = ethabi::decode(
                        &[
                            ethabi::ParamType::Uint(256usize),
                            ethabi::ParamType::Uint(256usize),
                            ethabi::ParamType::Uint(256usize),
                            ethabi::ParamType::Uint(256usize),
                        ],
                        log.data.as_ref(),
                    )
                    .map_err(|e| format!("unable to decode log.data: {}", e))?;
                values.reverse();
                Ok(Self {
                    sender: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[1usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'sender' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    recipient: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[2usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'recipient' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    amount0: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    amount1: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    paid0: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    paid1: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => panic!("Unable to decode logs.Flash event: {:#}", e),
                }
            }
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct IncreaseObservationCardinalityNext {
            pub observation_cardinality_next_old: ethabi::Uint,
            pub observation_cardinality_next_new: ethabi::Uint,
        }
        impl IncreaseObservationCardinalityNext {
            const TOPIC_ID: [u8; 32] = [
                172u8,
                73u8,
                229u8,
                24u8,
                249u8,
                10u8,
                53u8,
                143u8,
                101u8,
                46u8,
                68u8,
                0u8,
                22u8,
                79u8,
                5u8,
                165u8,
                216u8,
                247u8,
                227u8,
                94u8,
                119u8,
                71u8,
                39u8,
                155u8,
                195u8,
                169u8,
                61u8,
                191u8,
                88u8,
                78u8,
                18u8,
                90u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 1usize {
                    return false;
                }
                if log.data.len() != 64usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                let mut values = ethabi::decode(
                        &[
                            ethabi::ParamType::Uint(16usize),
                            ethabi::ParamType::Uint(16usize),
                        ],
                        log.data.as_ref(),
                    )
                    .map_err(|e| format!("unable to decode log.data: {}", e))?;
                values.reverse();
                Ok(Self {
                    observation_cardinality_next_old: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    observation_cardinality_next_new: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => {
                        panic!(
                            "Unable to decode logs.IncreaseObservationCardinalityNext event: {:#}",
                            e
                        )
                    }
                }
            }
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct Initialize {
            pub sqrt_price_x96: ethabi::Uint,
            pub tick: num_bigint::BigInt,
        }
        impl Initialize {
            const TOPIC_ID: [u8; 32] = [
                152u8,
                99u8,
                96u8,
                54u8,
                203u8,
                102u8,
                169u8,
                193u8,
                154u8,
                55u8,
                67u8,
                94u8,
                252u8,
                30u8,
                144u8,
                20u8,
                33u8,
                144u8,
                33u8,
                78u8,
                138u8,
                190u8,
                184u8,
                33u8,
                189u8,
                186u8,
                63u8,
                41u8,
                144u8,
                221u8,
                76u8,
                149u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 1usize {
                    return false;
                }
                if log.data.len() != 64usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                let mut values = ethabi::decode(
                        &[
                            ethabi::ParamType::Uint(160usize),
                            ethabi::ParamType::Int(24usize),
                        ],
                        log.data.as_ref(),
                    )
                    .map_err(|e| format!("unable to decode log.data: {}", e))?;
                values.reverse();
                Ok(Self {
                    sqrt_price_x96: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    tick: {
                        values.pop().expect(INTERNAL_ERR);
                        num_bigint::BigInt::from_signed_bytes_be(
                            log.data[32usize..64usize].as_ref(),
                        )
                    },
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => panic!("Unable to decode logs.Initialize event: {:#}", e),
                }
            }
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct Mint {
            pub sender: Vec<u8>,
            pub owner: Vec<u8>,
            pub tick_lower: num_bigint::BigInt,
            pub tick_upper: num_bigint::BigInt,
            pub amount: ethabi::Uint,
            pub amount0: ethabi::Uint,
            pub amount1: ethabi::Uint,
        }
        impl Mint {
            const TOPIC_ID: [u8; 32] = [
                122u8,
                83u8,
                8u8,
                11u8,
                164u8,
                20u8,
                21u8,
                139u8,
                231u8,
                236u8,
                105u8,
                185u8,
                135u8,
                181u8,
                251u8,
                125u8,
                7u8,
                222u8,
                225u8,
                1u8,
                254u8,
                133u8,
                72u8,
                143u8,
                8u8,
                83u8,
                174u8,
                22u8,
                35u8,
                157u8,
                11u8,
                222u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 4usize {
                    return false;
                }
                if log.data.len() != 128usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                let mut values = ethabi::decode(
                        &[
                            ethabi::ParamType::Address,
                            ethabi::ParamType::Uint(128usize),
                            ethabi::ParamType::Uint(256usize),
                            ethabi::ParamType::Uint(256usize),
                        ],
                        log.data.as_ref(),
                    )
                    .map_err(|e| format!("unable to decode log.data: {}", e))?;
                values.reverse();
                Ok(Self {
                    owner: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[1usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'owner' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    tick_lower: num_bigint::BigInt::from_signed_bytes_be(
                        log.topics[2usize].as_ref(),
                    ),
                    tick_upper: num_bigint::BigInt::from_signed_bytes_be(
                        log.topics[3usize].as_ref(),
                    ),
                    sender: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    amount: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    amount0: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    amount1: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => panic!("Unable to decode logs.Mint event: {:#}", e),
                }
            }
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct SetFeeProtocol {
            pub fee_protocol0_old: ethabi::Uint,
            pub fee_protocol1_old: ethabi::Uint,
            pub fee_protocol0_new: ethabi::Uint,
            pub fee_protocol1_new: ethabi::Uint,
        }
        impl SetFeeProtocol {
            const TOPIC_ID: [u8; 32] = [
                151u8,
                61u8,
                141u8,
                146u8,
                187u8,
                41u8,
                159u8,
                74u8,
                246u8,
                206u8,
                73u8,
                181u8,
                42u8,
                138u8,
                219u8,
                133u8,
                174u8,
                70u8,
                185u8,
                242u8,
                20u8,
                196u8,
                196u8,
                252u8,
                6u8,
                172u8,
                119u8,
                64u8,
                18u8,
                55u8,
                177u8,
                51u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 1usize {
                    return false;
                }
                if log.data.len() != 128usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                let mut values = ethabi::decode(
                        &[
                            ethabi::ParamType::Uint(8usize),
                            ethabi::ParamType::Uint(8usize),
                            ethabi::ParamType::Uint(8usize),
                            ethabi::ParamType::Uint(8usize),
                        ],
                        log.data.as_ref(),
                    )
                    .map_err(|e| format!("unable to decode log.data: {}", e))?;
                values.reverse();
                Ok(Self {
                    fee_protocol0_old: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    fee_protocol1_old: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    fee_protocol0_new: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    fee_protocol1_new: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => {
                        panic!("Unable to decode logs.SetFeeProtocol event: {:#}", e)
                    }
                }
            }
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct Swap {
            pub sender: Vec<u8>,
            pub recipient: Vec<u8>,
            pub amount0: num_bigint::BigInt,
            pub amount1: num_bigint::BigInt,
            pub sqrt_price_x96: ethabi::Uint,
            pub liquidity: ethabi::Uint,
            pub tick: num_bigint::BigInt,
        }
        impl Swap {
            const TOPIC_ID: [u8; 32] = [
                196u8,
                32u8,
                121u8,
                249u8,
                74u8,
                99u8,
                80u8,
                215u8,
                230u8,
                35u8,
                95u8,
                41u8,
                23u8,
                73u8,
                36u8,
                249u8,
                40u8,
                204u8,
                42u8,
                200u8,
                24u8,
                235u8,
                100u8,
                254u8,
                216u8,
                0u8,
                78u8,
                17u8,
                95u8,
                188u8,
                202u8,
                103u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 3usize {
                    return false;
                }
                if log.data.len() != 160usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                let mut values = ethabi::decode(
                        &[
                            ethabi::ParamType::Int(256usize),
                            ethabi::ParamType::Int(256usize),
                            ethabi::ParamType::Uint(160usize),
                            ethabi::ParamType::Uint(128usize),
                            ethabi::ParamType::Int(24usize),
                        ],
                        log.data.as_ref(),
                    )
                    .map_err(|e| format!("unable to decode log.data: {}", e))?;
                values.reverse();
                Ok(Self {
                    sender: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[1usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'sender' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    recipient: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[2usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'recipient' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    amount0: {
                        values.pop().expect(INTERNAL_ERR);
                        num_bigint::BigInt::from_signed_bytes_be(
                            log.data[0usize..32usize].as_ref(),
                        )
                    },
                    amount1: {
                        values.pop().expect(INTERNAL_ERR);
                        num_bigint::BigInt::from_signed_bytes_be(
                            log.data[32usize..64usize].as_ref(),
                        )
                    },
                    sqrt_price_x96: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    liquidity: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    tick: {
                        values.pop().expect(INTERNAL_ERR);
                        num_bigint::BigInt::from_signed_bytes_be(
                            log.data[128usize..160usize].as_ref(),
                        )
                    },
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => panic!("Unable to decode logs.Swap event: {:#}", e),
                }
            }
        }
    }