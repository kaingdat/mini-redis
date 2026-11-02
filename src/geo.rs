use std::{cmp::Ordering, sync::Arc};

use bytes::Bytes;
use dashmap::DashMap;

use crate::{
    command::{WRONGTYPE, parse_score, wrong_arity},
    types::RedisValueRef,
    value::{RedisValue, SortedSetData, ValueEntry},
};

const MIN_LAT: f64 = -85.05112878;
const MAX_LAT: f64 = 85.05112878;
const MIN_LON: f64 = -180.0;
const MAX_LON: f64 = 180.0;
const LAT_RANGE: f64 = MAX_LAT - MIN_LAT;
const LON_RANGE: f64 = MAX_LON - MIN_LON;
const STEPS: u32 = 26;
const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;
const MERCATOR_MAX: f64 = 20037726.37;
const D_R: f64 = std::f64::consts::PI / 180.0;

pub fn is_valid(lon: f64, lat: f64) -> bool {
    (MIN_LON..=MAX_LON).contains(&lon) && (MIN_LAT..=MAX_LAT).contains(&lat)
}

fn spread(v: u32) -> u64 {
    let mut r = u64::from(v);
    r = (r | (r << 16)) & 0x0000_FFFF_0000_FFFF;
    r = (r | (r << 8)) & 0x00FF_00FF_00FF_00FF;
    r = (r | (r << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    r = (r | (r << 2)) & 0x3333_3333_3333_3333;
    (r | (r << 1)) & 0x5555_5555_5555_5555
}

fn compact(v: u64) -> u32 {
    let mut r = v & 0x5555_5555_5555_5555;
    r = (r | (r >> 1)) & 0x3333_3333_3333_3333;
    r = (r | (r >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    r = (r | (r >> 4)) & 0x00FF_00FF_00FF_00FF;
    r = (r | (r >> 8)) & 0x0000_FFFF_0000_FFFF;
    ((r | (r >> 16)) & 0x0000_0000_FFFF_FFFF) as u32
}

fn interleave(lat_idx: u32, lon_idx: u32) -> u64 {
    spread(lat_idx) | (spread(lon_idx) << 1)
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Cell {
    lat: u32,
    lon: u32,
    step: u32,
}

impl Cell {
    fn containing(lon: f64, lat: f64, step: u32) -> Self {
        let scale = (1u64 << step) as f64;
        Self {
            lat: ((lat - MIN_LAT) / LAT_RANGE * scale) as u32,
            lon: ((lon - MIN_LON) / LON_RANGE * scale) as u32,
            step,
        }
    }

    fn moved(self, d_lon: i8, d_lat: i8) -> Self {
        let mask = ((1u64 << self.step) - 1) as u32;
        let shift = |idx: u32, d: i8| match d {
            0 => idx,
            1 => idx.wrapping_add(1) & mask,
            _ => idx.wrapping_sub(1) & mask,
        };
        Self {
            lat: shift(self.lat, d_lat),
            lon: shift(self.lon, d_lon),
            step: self.step,
        }
    }

    fn area(self) -> Area {
        let scale = (1u64 << self.step) as f64;
        let lat = f64::from(self.lat);
        let lon = f64::from(self.lon);
        Area {
            lat_min: MIN_LAT + (lat / scale) * LAT_RANGE,
            lat_max: MIN_LAT + ((lat + 1.0) / scale) * LAT_RANGE,
            lon_min: MIN_LON + (lon / scale) * LON_RANGE,
            lon_max: MIN_LON + ((lon + 1.0) / scale) * LON_RANGE,
        }
    }

    fn score_range(self) -> (f64, f64) {
        let shift = 2 * (STEPS - self.step);
        let bits = interleave(self.lat, self.lon);
        ((bits << shift) as f64, ((bits + 1) << shift) as f64)
    }
}

struct Area {
    lat_min: f64,
    lat_max: f64,
    lon_min: f64,
    lon_max: f64,
}

pub fn encode(lon: f64, lat: f64) -> u64 {
    let cell = Cell::containing(lon, lat, STEPS);
    interleave(cell.lat, cell.lon)
}

pub fn decode(hash: u64) -> (f64, f64) {
    let area = Cell {
        lat: compact(hash),
        lon: compact(hash >> 1),
        step: STEPS,
    }
    .area();
    let lon = ((area.lon_min + area.lon_max) / 2.0).clamp(MIN_LON, MAX_LON);
    let lat = ((area.lat_min + area.lat_max) / 2.0).clamp(MIN_LAT, MAX_LAT);
    (lon, lat)
}

pub fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1r = lat1 * D_R;
    let lat2r = lat2 * D_R;
    let v = ((lon2 * D_R - lon1 * D_R) / 2.0).sin();
    if v == 0.0 {
        return EARTH_RADIUS_IN_METERS * (lat2r - lat1r).abs();
    }
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

fn estimate_steps(radius: f64, lat: f64) -> u32 {
    if radius == 0.0 {
        return STEPS;
    }
    let mut range = radius;
    let mut step: i32 = 1;
    while range < MERCATOR_MAX {
        range *= 2.0;
        step += 1;
    }
    step -= 2;
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }
    step.clamp(1, STEPS as i32) as u32
}

fn bounding_box(lon: f64, lat: f64, radius: f64) -> Area {
    let lat_delta = (radius / EARTH_RADIUS_IN_METERS) / D_R;
    let lon_delta_top = (radius / EARTH_RADIUS_IN_METERS / ((lat + lat_delta) * D_R).cos()) / D_R;
    let lon_delta_bottom =
        (radius / EARTH_RADIUS_IN_METERS / ((lat - lat_delta) * D_R).cos()) / D_R;
    let lon_delta = if lat < 0.0 {
        lon_delta_bottom
    } else {
        lon_delta_top
    };
    Area {
        lat_min: lat - lat_delta,
        lat_max: lat + lat_delta,
        lon_min: lon - lon_delta,
        lon_max: lon + lon_delta,
    }
}

fn search_ranges(lon: f64, lat: f64, radius: f64) -> Vec<(f64, f64)> {
    let bounds = bounding_box(lon, lat, radius);
    if bounds.lat_max > 90.0 || bounds.lat_min < -90.0 {
        return vec![(f64::NEG_INFINITY, f64::INFINITY)];
    }
    let mut step = estimate_steps(radius, lat);
    let mut center = Cell::containing(lon, lat, step);

    let misses_bounds = |c: Cell| {
        c.moved(0, 1).area().lat_max < bounds.lat_max
            || c.moved(0, -1).area().lat_min > bounds.lat_min
            || c.moved(1, 0).area().lon_max < bounds.lon_max
            || c.moved(-1, 0).area().lon_min > bounds.lon_min
    };
    if step > 1 && misses_bounds(center) {
        step -= 1;
        center = Cell::containing(lon, lat, step);
    }

    let area = center.area();
    let mut drop_south = false;
    let mut drop_north = false;
    let mut drop_west = false;
    let mut drop_east = false;
    if step >= 2 {
        drop_south = area.lat_min < bounds.lat_min;
        drop_north = area.lat_max > bounds.lat_max;
        drop_west = area.lon_min < bounds.lon_min;
        drop_east = area.lon_max > bounds.lon_max;
    }

    let order: [(i8, i8); 9] = [
        (0, 0),
        (0, 1),
        (0, -1),
        (1, 0),
        (-1, 0),
        (1, 1),
        (-1, 1),
        (1, -1),
        (-1, -1),
    ];
    let mut cells: Vec<Cell> = Vec::with_capacity(9);
    for (d_lon, d_lat) in order {
        let dropped = (d_lat == -1 && drop_south)
            || (d_lat == 1 && drop_north)
            || (d_lon == -1 && drop_west)
            || (d_lon == 1 && drop_east);
        let cell = center.moved(d_lon, d_lat);
        if !dropped && !cells.contains(&cell) {
            cells.push(cell);
        }
    }
    cells.into_iter().map(Cell::score_range).collect()
}

pub fn search(zset: &SortedSetData, lon: f64, lat: f64, radius: f64) -> Vec<(Bytes, f64)> {
    let mut found = Vec::new();
    for (min, max) in search_ranges(lon, lat, radius) {
        for (member, score) in zset.range_by_score(min, max) {
            let (m_lon, m_lat) = decode(score as u64);
            let d = distance(lon, lat, m_lon, m_lat);
            if d <= radius {
                found.push((member.clone(), d));
            }
        }
    }
    found
}

pub fn format_coordinate(value: f64) -> String {
    if value == value.trunc() && value.abs() < 1e15 {
        return format!("{}", value as i64);
    }
    let sci = format!("{value:e}");
    let (mantissa, exponent) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let negative = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let ndigits = digits.len() as i32;
    let k = exponent - (ndigits - 1);
    let exp_abs = exponent.abs();

    let mut out = String::new();
    if negative {
        out.push('-');
    }
    if k >= 0 && exp_abs < ndigits + 7 {
        out.push_str(&digits);
        out.extend(std::iter::repeat_n('0', k as usize));
    } else if k < 0 && (k > -7 || exp_abs < 4) {
        let offset = ndigits + k;
        if offset <= 0 {
            out.push_str("0.");
            out.extend(std::iter::repeat_n('0', (-offset) as usize));
            out.push_str(&digits);
        } else {
            out.push_str(&digits[..offset as usize]);
            out.push('.');
            out.push_str(&digits[offset as usize..]);
        }
    } else {
        out.push_str(&digits[..1]);
        if ndigits > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exponent < 0 { '-' } else { '+' });
        out.push_str(&exp_abs.to_string());
    }
    out
}

pub fn format_distance(value: f64) -> String {
    let scaled = (value * 10_000.0).round_ties_even() as i64;
    let sign = if scaled < 0 { "-" } else { "" };
    let abs = scaled.unsigned_abs();
    format!("{sign}{}.{:04}", abs / 10_000, abs % 10_000)
}

fn error(msg: &str) -> RedisValueRef {
    RedisValueRef::ErrorMsg(msg.as_bytes().to_vec())
}

fn syntax_error() -> RedisValueRef {
    error("ERR syntax error")
}

fn arg(parts: &[RedisValueRef], i: usize) -> &[u8] {
    match parts.get(i) {
        Some(RedisValueRef::BulkString(b)) => b,
        _ => b"",
    }
}

fn parse_float(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| parse_score(s).ok())
}

fn parse_lon_lat(parts: &[RedisValueRef], i: usize) -> Result<(f64, f64), RedisValueRef> {
    let invalid_float = || error("ERR value is not a valid float");
    let lon = parse_float(arg(parts, i)).ok_or_else(invalid_float)?;
    let lat = parse_float(arg(parts, i + 1)).ok_or_else(invalid_float)?;
    if !is_valid(lon, lat) {
        return Err(error(&format!(
            "ERR invalid longitude,latitude pair {lon:.6},{lat:.6}"
        )));
    }
    Ok((lon, lat))
}

fn parse_unit(bytes: &[u8]) -> Result<f64, RedisValueRef> {
    match bytes.to_ascii_lowercase().as_slice() {
        b"m" => Ok(1.0),
        b"km" => Ok(1000.0),
        b"ft" => Ok(0.3048),
        b"mi" => Ok(1609.34),
        _ => Err(error(
            "ERR unsupported unit provided. please use M, KM, FT, MI",
        )),
    }
}

fn with_zset<T>(
    storage: &DashMap<Bytes, ValueEntry>,
    key: &[u8],
    f: impl FnOnce(Option<&SortedSetData>) -> T,
) -> Result<T, RedisValueRef> {
    storage.remove_if(key, |_, entry| entry.is_expired());
    match storage.get(key) {
        Some(entry) => match &entry.data {
            RedisValue::SortedSet(zset) => Ok(f(Some(zset))),
            _ => Err(RedisValueRef::ErrorMsg(WRONGTYPE.to_vec())),
        },
        None => Ok(f(None)),
    }
}

fn bulk(s: String) -> RedisValueRef {
    RedisValueRef::BulkString(Bytes::from(s))
}

pub fn handle_geoadd(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    if parts.len() < 5 {
        return wrong_arity("geoadd");
    }
    if !(parts.len() - 2).is_multiple_of(3) {
        return syntax_error();
    }
    let key = Bytes::copy_from_slice(arg(parts, 1));

    let mut points = Vec::with_capacity((parts.len() - 2) / 3);
    for i in (2..parts.len()).step_by(3) {
        let (lon, lat) = match parse_lon_lat(parts, i) {
            Ok(point) => point,
            Err(e) => return e,
        };
        points.push((Bytes::copy_from_slice(arg(parts, i + 2)), encode(lon, lat)));
    }

    storage.remove_if(&key, |_, entry| entry.is_expired());
    let mut entry = storage
        .entry(key)
        .or_insert_with(|| ValueEntry::new(RedisValue::SortedSet(SortedSetData::new()), None));
    let RedisValue::SortedSet(zset) = &mut entry.data else {
        return RedisValueRef::ErrorMsg(WRONGTYPE.to_vec());
    };
    let added = points
        .into_iter()
        .filter(|(member, hash)| zset.add(member.clone(), *hash as f64))
        .count();
    RedisValueRef::Int(added as i64)
}

pub fn handle_geopos(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    if parts.len() < 2 {
        return wrong_arity("geopos");
    }
    let reply = with_zset(storage, arg(parts, 1), |zset| {
        (2..parts.len())
            .map(|i| {
                let member = Bytes::copy_from_slice(arg(parts, i));
                match zset.and_then(|z| z.score(&member)) {
                    Some(score) => {
                        let (lon, lat) = decode(score as u64);
                        RedisValueRef::Array(vec![
                            bulk(format_coordinate(lon)),
                            bulk(format_coordinate(lat)),
                        ])
                    }
                    None => RedisValueRef::NullArray,
                }
            })
            .collect()
    });
    match reply {
        Ok(items) => RedisValueRef::Array(items),
        Err(e) => e,
    }
}

pub fn handle_geodist(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    if parts.len() < 4 {
        return wrong_arity("geodist");
    }
    let to_meters = match parts.len() {
        4 => 1.0,
        5 => match parse_unit(arg(parts, 4)) {
            Ok(unit) => unit,
            Err(e) => return e,
        },
        _ => return syntax_error(),
    };
    let first = Bytes::copy_from_slice(arg(parts, 2));
    let second = Bytes::copy_from_slice(arg(parts, 3));
    let reply = with_zset(storage, arg(parts, 1), |zset| {
        let zset = zset?;
        let (lon1, lat1) = decode(zset.score(&first)? as u64);
        let (lon2, lat2) = decode(zset.score(&second)? as u64);
        Some(distance(lon1, lat1, lon2, lat2) / to_meters)
    });
    match reply {
        Ok(Some(d)) => bulk(format_distance(d)),
        Ok(None) => RedisValueRef::NullBulkString,
        Err(e) => e,
    }
}

struct SearchArgs {
    center: Option<(f64, f64)>,
    radius: Option<(f64, f64)>,
    order: Option<Ordering>,
}

fn parse_search_args(parts: &[RedisValueRef]) -> Result<SearchArgs, RedisValueRef> {
    let mut args = SearchArgs {
        center: None,
        radius: None,
        order: None,
    };
    let mut i = 2;
    while i < parts.len() {
        let word = arg(parts, i).to_ascii_lowercase();
        let has_two = i + 2 < parts.len();
        match word.as_slice() {
            b"fromlonlat" if has_two => {
                args.center = Some(parse_lon_lat(parts, i + 1)?);
                i += 3;
            }
            b"byradius" if has_two => {
                let radius = parse_float(arg(parts, i + 1))
                    .ok_or_else(|| error("ERR need numeric radius"))?;
                if radius < 0.0 {
                    return Err(error("ERR radius cannot be negative"));
                }
                args.radius = Some((radius, parse_unit(arg(parts, i + 2))?));
                i += 3;
            }
            b"asc" => {
                args.order = Some(Ordering::Less);
                i += 1;
            }
            b"desc" => {
                args.order = Some(Ordering::Greater);
                i += 1;
            }
            _ => return Err(syntax_error()),
        }
    }
    let name = String::from_utf8_lossy(arg(parts, 0));
    if args.center.is_none() {
        return Err(error(&format!(
            "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for {name}"
        )));
    }
    if args.radius.is_none() {
        return Err(error(&format!(
            "ERR exactly one of BYRADIUS and BYBOX can be specified for {name}"
        )));
    }
    Ok(args)
}

pub fn handle_geosearch(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    if parts.len() < 7 {
        return wrong_arity("geosearch");
    }
    let key = arg(parts, 1);
    if let Err(e) = with_zset(storage, key, |_| ()) {
        return e;
    }
    let args = match parse_search_args(parts) {
        Ok(args) => args,
        Err(e) => return e,
    };
    let (Some((lon, lat)), Some((radius, unit))) = (args.center, args.radius) else {
        return syntax_error();
    };
    let reply = with_zset(storage, key, |zset| {
        let Some(zset) = zset else {
            return Vec::new();
        };
        let mut found = search(zset, lon, lat, radius * unit);
        if let Some(order) = args.order {
            found.sort_by(|a, b| {
                let ord = a.1.total_cmp(&b.1);
                if order == Ordering::Less {
                    ord
                } else {
                    ord.reverse()
                }
            });
        }
        found
            .into_iter()
            .map(|(member, _)| RedisValueRef::BulkString(member))
            .collect()
    });
    match reply {
        Ok(items) => RedisValueRef::Array(items),
        Err(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use rand::{RngExt, SeedableRng, rngs::SmallRng};

    use super::*;

    const CITIES: [(&str, f64, f64, u64, f64, f64); 12] = [
        (
            "Bangkok",
            13.7220,
            100.5252,
            3962257306574459,
            13.722000686932997,
            100.52520006895065,
        ),
        (
            "Beijing",
            39.9075,
            116.3972,
            4069885364908765,
            39.9075003315814,
            116.39719873666763,
        ),
        (
            "Berlin",
            52.5244,
            13.4105,
            3673983964876493,
            52.52439934649943,
            13.410500586032867,
        ),
        (
            "Copenhagen",
            55.6759,
            12.5655,
            3685973395504349,
            55.67589927498264,
            12.56549745798111,
        ),
        (
            "New Delhi",
            28.6667,
            77.2167,
            3631527070936756,
            28.666698899347338,
            77.21670180559158,
        ),
        (
            "Kathmandu",
            27.7017,
            85.3206,
            3639507404773204,
            27.701700137333084,
            85.3205993771553,
        ),
        (
            "London",
            51.5074,
            -0.1278,
            2163557714755072,
            51.50740077990134,
            -0.12779921293258667,
        ),
        (
            "New York",
            40.7128,
            -74.0060,
            1791873974549446,
            40.712798986951505,
            -74.00600105524063,
        ),
        (
            "Paris",
            48.8534,
            2.3488,
            3663832752681684,
            48.85340071224621,
            2.348802387714386,
        ),
        (
            "Sydney",
            -33.8688,
            151.2093,
            3252046221964352,
            -33.86880091934156,
            151.2092998623848,
        ),
        (
            "Tokyo",
            35.6895,
            139.6917,
            4171231230197045,
            35.68950126697936,
            139.691701233387,
        ),
        (
            "Vienna",
            48.2064,
            16.3707,
            3673109836391743,
            48.20640046271915,
            16.370699107646942,
        ),
    ];

    #[test]
    fn encodes_and_decodes_reference_cities() {
        for (name, lat, lon, score, dec_lat, dec_lon) in CITIES {
            assert_eq!(encode(lon, lat), score, "{name}");
            let (got_lon, got_lat) = decode(score);
            assert!((got_lat - dec_lat).abs() < 1e-6, "{name}");
            assert!((got_lon - dec_lon).abs() < 1e-6, "{name}");
        }
    }

    #[test]
    fn scores_survive_a_round_trip_through_f64() {
        for (_, lat, lon, ..) in CITIES {
            let hash = encode(lon, lat);
            assert_eq!(hash as f64 as u64, hash);
        }
    }

    #[test]
    fn distances_match_redis() {
        let (lon1, lat1) = decode(encode(13.361389, 38.115556));
        let (lon2, lat2) = decode(encode(15.087269, 37.502669));
        let d = distance(lon1, lat1, lon2, lat2);
        assert_eq!(format_distance(d), "166274.1516");
        assert_eq!(format_distance(d / 1000.0), "166.2742");
        assert_eq!(format_distance(distance(lon1, lat1, lon1, lat1)), "0.0000");
        let same_lon = distance(lon1, 10.0, lon1, 20.0);
        let u = ((20.0 * D_R - 10.0 * D_R) / 2.0).sin();
        let full = 2.0 * EARTH_RADIUS_IN_METERS * (u * u).sqrt().asin();
        assert!((same_lon - full).abs() < 1e-6);
    }

    #[test]
    fn coordinates_format_like_redis() {
        assert_eq!(format_coordinate(13.361389338970184), "13.361389338970184");
        assert_eq!(format_coordinate(38.1155563954963), "38.1155563954963");
        assert_eq!(format_coordinate(180.0), "180");
        assert_eq!(
            format_coordinate(-179.99999731779099),
            "-179.99999731779099"
        );
        assert_eq!(
            format_coordinate(2.682209014892578e-6),
            "2.682209014892578e-6"
        );
        assert_eq!(
            format_coordinate(1.0012148579388857e-4),
            "1.0012148579388857e-4"
        );
        assert_eq!(
            format_coordinate(0.0010004639625549316),
            "0.0010004639625549316"
        );
        assert_eq!(
            format_coordinate(-9.99947497359216e-4),
            "-9.99947497359216e-4"
        );
        assert_eq!(format_coordinate(1.5e-5), "0.000015");
    }

    fn brute_force(points: &[(Bytes, f64, f64)], lon: f64, lat: f64, r: f64) -> Vec<Bytes> {
        let mut out: Vec<Bytes> = points
            .iter()
            .filter(|(_, p_lon, p_lat)| {
                let (d_lon, d_lat) = decode(encode(*p_lon, *p_lat));
                distance(lon, lat, d_lon, d_lat) <= r
            })
            .map(|(m, ..)| m.clone())
            .collect();
        out.sort();
        out
    }

    fn random_center(rng: &mut SmallRng) -> (f64, f64) {
        let lon = match rng.random_range(0..4) {
            0 => 180.0 - rng.random_range(0.0..1.0),
            1 => -180.0 + rng.random_range(0.0..1.0),
            _ => rng.random_range(MIN_LON..=MAX_LON),
        };
        let lat = match rng.random_range(0..4) {
            0 => MAX_LAT - rng.random_range(0.0..1.0),
            1 => MIN_LAT + rng.random_range(0.0..1.0),
            _ => rng.random_range(MIN_LAT..=MAX_LAT),
        };
        (lon, lat)
    }

    #[test]
    fn cell_search_matches_a_full_scan() {
        let mut rng = SmallRng::seed_from_u64(8);
        let mut zset = SortedSetData::new();
        let mut points = Vec::new();
        for i in 0..5_000 {
            let (lon, lat) = random_center(&mut rng);
            let member = Bytes::from(format!("p{i}"));
            zset.add(member.clone(), encode(lon, lat) as f64);
            points.push((member, lon, lat));
        }
        for _ in 0..2_000 {
            let (lon, lat) = random_center(&mut rng);
            let radius = rng.random_range(0.0..5_000_000.0);
            let mut got: Vec<Bytes> = search(&zset, lon, lat, radius)
                .into_iter()
                .map(|(m, _)| m)
                .collect();
            got.sort();
            let want = brute_force(&points, lon, lat, radius);
            assert_eq!(got, want, "lon={lon} lat={lat} radius={radius}");
        }
    }

    #[test]
    fn radius_edge_cases() {
        let mut rng = SmallRng::seed_from_u64(9);
        let mut zset = SortedSetData::new();
        for i in 0..500 {
            let lon = rng.random_range(MIN_LON..=MAX_LON);
            let lat = rng.random_range(MIN_LAT..=MAX_LAT);
            zset.add(Bytes::from(format!("p{i}")), encode(lon, lat) as f64);
        }
        assert_eq!(search(&zset, 0.0, 0.0, 30_000_000.0).len(), 500);

        let hash = encode(12.5, 41.9);
        zset.add(Bytes::from_static(b"rome"), hash as f64);
        let (lon, lat) = decode(hash);
        let hits = search(&zset, lon, lat, 0.0);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, Bytes::from_static(b"rome"));
        assert!(search(&zset, lon + 0.001, lat, 0.0).is_empty());
    }

    #[test]
    fn no_member_is_returned_twice() {
        let mut zset = SortedSetData::new();
        for (i, (_, lat, lon, ..)) in CITIES.iter().enumerate() {
            zset.add(Bytes::from(format!("c{i}")), encode(*lon, *lat) as f64);
        }
        for radius in [1.0, 1e5, 1e6, 5e6, 1e7, 3e7] {
            let mut found: Vec<_> = search(&zset, 0.0, 0.0, radius)
                .into_iter()
                .map(|(m, _)| m)
                .collect();
            let len = found.len();
            found.sort();
            found.dedup();
            assert_eq!(found.len(), len);
        }
    }
}
