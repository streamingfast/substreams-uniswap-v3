    const INTERNAL_ERR: &'static str = "`ethabi_derive` internal error";
    /// Contract's events.
    #[allow(dead_code)]
    pub mod events {
        use super::INTERNAL_ERR;
        #[derive(Debug, Clone, PartialEq)]
        pub struct FeeAmountEnabled {
            pub fee: ethabi::Uint,
            pub tick_spacing: ethabi::Int,
        }
        impl FeeAmountEnabled {
            const TOPIC_ID: [u8; 32] = [
                198u8,
                106u8,
                63u8,
                223u8,
                7u8,
                35u8,
                44u8,
                221u8,
                24u8,
                95u8,
                235u8,
                204u8,
                101u8,
                121u8,
                212u8,
                8u8,
                194u8,
                65u8,
                180u8,
                122u8,
                226u8,
                249u8,
                144u8,
                125u8,
                132u8,
                190u8,
                101u8,
                81u8,
                65u8,
                238u8,
                174u8,
                204u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 3usize {
                    return false;
                }
                if log.data.len() != 0usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                Ok(Self {
                    fee: ethabi::decode(
                            &[ethabi::ParamType::Uint(24usize)],
                            log.topics[1usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'fee' from topic of type 'uint24': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    tick_spacing: ethabi::decode(
                            &[ethabi::ParamType::Int(24usize)],
                            log.topics[2usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'tick_spacing' from topic of type 'int24': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_int()
                        .expect(INTERNAL_ERR),
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => {
                        panic!("Unable to decode logs.FeeAmountEnabled event: {:#}", e)
                    }
                }
            }
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct OwnerChanged {
            pub old_owner: Vec<u8>,
            pub new_owner: Vec<u8>,
        }
        impl OwnerChanged {
            const TOPIC_ID: [u8; 32] = [
                181u8,
                50u8,
                7u8,
                59u8,
                56u8,
                200u8,
                49u8,
                69u8,
                227u8,
                229u8,
                19u8,
                83u8,
                119u8,
                160u8,
                139u8,
                249u8,
                170u8,
                181u8,
                91u8,
                192u8,
                253u8,
                124u8,
                17u8,
                121u8,
                205u8,
                79u8,
                185u8,
                149u8,
                210u8,
                165u8,
                21u8,
                156u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 3usize {
                    return false;
                }
                if log.data.len() != 0usize {
                    return false;
                }
                return log.topics.get(0).expect("bounds already checked").as_ref()
                    == Self::TOPIC_ID;
            }
            pub fn decode(
                log: &substreams_ethereum::pb::eth::v1::Log,
            ) -> Result<Self, String> {
                Ok(Self {
                    old_owner: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[1usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'old_owner' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .expect(INTERNAL_ERR)
                        .as_bytes()
                        .to_vec(),
                    new_owner: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[2usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'new_owner' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .expect(INTERNAL_ERR)
                        .as_bytes()
                        .to_vec(),
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => panic!("Unable to decode logs.OwnerChanged event: {:#}", e),
                }
            }
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct PoolCreated {
            pub token0: Vec<u8>,
            pub token1: Vec<u8>,
            pub fee: ethabi::Uint,
            pub tick_spacing: ethabi::Int,
            pub pool: Vec<u8>,
        }
        impl PoolCreated {
            const TOPIC_ID: [u8; 32] = [
                120u8,
                60u8,
                202u8,
                28u8,
                4u8,
                18u8,
                221u8,
                13u8,
                105u8,
                94u8,
                120u8,
                69u8,
                104u8,
                201u8,
                109u8,
                162u8,
                233u8,
                194u8,
                47u8,
                249u8,
                137u8,
                53u8,
                122u8,
                46u8,
                139u8,
                29u8,
                155u8,
                43u8,
                78u8,
                107u8,
                113u8,
                24u8,
            ];
            pub fn match_log(log: &substreams_ethereum::pb::eth::v1::Log) -> bool {
                if log.topics.len() != 4usize {
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
                        &[ethabi::ParamType::Int(24usize), ethabi::ParamType::Address],
                        log.data.as_ref(),
                    )
                    .map_err(|e| format!("unable to decode log.data: {}", e))?;
                Ok(Self {
                    token0: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[1usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'token0' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .expect(INTERNAL_ERR)
                        .as_bytes()
                        .to_vec(),
                    token1: ethabi::decode(
                            &[ethabi::ParamType::Address],
                            log.topics[2usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'token1' from topic of type 'address': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .expect(INTERNAL_ERR)
                        .as_bytes()
                        .to_vec(),
                    fee: ethabi::decode(
                            &[ethabi::ParamType::Uint(24usize)],
                            log.topics[3usize].as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "unable to decode param 'fee' from topic of type 'uint24': {}",
                                e
                            )
                        })?
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_uint()
                        .expect(INTERNAL_ERR),
                    pool: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_address()
                        .expect(INTERNAL_ERR)
                        .as_bytes()
                        .to_vec(),
                    tick_spacing: values
                        .pop()
                        .expect(INTERNAL_ERR)
                        .into_int()
                        .expect(INTERNAL_ERR),
                })
            }
            pub fn must_decode(log: &substreams_ethereum::pb::eth::v1::Log) -> Self {
                match Self::decode(log) {
                    Ok(v) => v,
                    Err(e) => panic!("Unable to decode logs.PoolCreated event: {:#}", e),
                }
            }
        }
    }