# -*- coding: utf-8 -*-

import json
import logging
import subprocess
import boto3

from typing import Any, Dict, List
from task import get_source

SIGNED_URL_EXPIRATION = 300  # The number of seconds that the Signed URL is valid

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


class RecursiveNamespace:
    @staticmethod
    def map_entry(entry):
        if isinstance(entry, dict):
            return RecursiveNamespace(**entry)

        return entry

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            if type(val) == dict:
                setattr(self, key, RecursiveNamespace(**val))
            elif type(val) == list:
                setattr(self, key, list(map(self.map_entry, val)))
            else:  # this is the only addition
                setattr(self, key, val)


class _Decoder(json.JSONDecoder):
    def decode(self, s, **kwargs):
        result = super().decode(s)
        return self._decode(result)

    def _decode(self, o):
        if isinstance(o, str):
            if o.lower() == 'true':
                return True
            elif o.lower() == 'false':
                return False
            else:
                try:
                    return int(o)
                except ValueError:
                    try:
                        return float(o)
                    except ValueError:
                        return o
        elif isinstance(o, dict):
            return {k: self._decode(v) for k, v in o.items()}
        elif isinstance(o, list):
            return [self._decode(v) for v in o]
        else:
            return o


# noinspection PyUnresolvedReferences
class Track:
    """
    An object associated with a media file track.
    Each :class:`Track` attribute corresponds to attributes parsed from MediaInfo's output.
    All attributes are lower case. Attributes that are present several times such as `Duration`
    yield a second attribute starting with `other_` which is a list of all alternative
    attribute values.
    When a non-existing attribute is accessed, `None` is returned.

    Example:
    >>> t = mi.tracks[0]
    >>> t
    <Track track_id='None', track_type='General'>
    >>> t.duration
    3000
    >>> t.other_duration
    ['3 s 0 ms', '3 s 0 ms', '3 s 0 ms', '00:00:03.000', '00:00:03.000']
    >>> type(t.non_existing)
    NoneType
    All available attributes can be obtained by calling :func:`to_data`.
    """

    def __eq__(self, other):  # type: ignore
        return self.__dict__ == other.__dict__

    def __getattribute__(self, name):  # type: ignore
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            pass
        return None

    def __getstate__(self):  # type: ignore
        return self.__dict__

    def __setstate__(self, state):  # type: ignore
        self.__dict__ = state

    def __init__(self, track: dict):
        self.track_type = track["@type"]
        repeated_attributes = []
        for k, v in track.items():
            if k == "id":
                k = "track_id"

            if getattr(self, k) is None:
                if isinstance(v, dict):
                    v = json.loads(json.dumps(v), object_hook=lambda d: RecursiveNamespace(**d))

                setattr(self, k, v)
            else:
                other_name = f"other_{k}"
                repeated_attributes.append((k, other_name))
                if getattr(self, other_name) is None:
                    setattr(self, other_name, [v])
                else:
                    getattr(self, other_name).append(v)

        for primary_key, other_key in repeated_attributes:
            try:
                # Attempt to convert the main value to int
                # Usually, if an attribute is repeated, one of its value
                # is an int and others are human-readable formats
                setattr(self, primary_key, int(getattr(self, primary_key)))
            except ValueError:
                # If it fails, try to find a secondary value
                # that is an int and swap it with the main value
                for other_value in getattr(self, other_key):
                    try:
                        current = getattr(self, primary_key)
                        # Set the main value to an int
                        setattr(self, primary_key, int(other_value))
                        # Append its previous value to other values
                        getattr(self, other_key).append(current)
                        break
                    except ValueError:
                        pass

    def __repr__(self):  # type: ignore
        return "<Track track_id='{}', track_type='{}'>".format(self.track_id, self.track_type)

    def to_data(self) -> Dict[str, Any]:
        """
        Returns a dict representation of the track attributes.
        Example:
        >>> sorted(track.to_data().keys())[:3]
        ['codec', 'codec_extensions_usually_used', 'codec_url']
        >>> t.to_data()["file_size"]
        5988
        :rtype: dict
        """
        data = self.__dict__
        for k, v in data.items():
            if isinstance(v, RecursiveNamespace):
                data[k] = v.__dict__
        return data


# noinspection PyUnresolvedReferences
class MediaInfo:
    """
    An object containing information about a media file.

    :param str xml: XML output obtained from MediaInfo.
    :param str encoding_errors: option to pass to :func:`str.encode`'s `errors`
        parameter before parsing `xml`.
    :raises xml.etree.ElementTree.ParseError: if passed invalid XML.
    :var tracks: A list of :py:class:`Track` objects which the media file contains.
        For instance:
        >>> mi = MediaInfo(media_info_json_output)
        >>> for t in mi.tracks:
        ...     print(t)
        <Track track_id='None', track_type='General'>
        <Track track_id='1', track_type='Text'>
    """

    def __eq__(self, other):  # type: ignore
        return self.tracks == other.tracks

    def __init__(self, mi_output: dict):
        self.tracks = []

        for iter_track in mi_output.get('media', dict()).get('track', []):
            self.tracks.append(Track(iter_track))

    def _tracks(self, track_type: str) -> List[Track]:
        return [track for track in self.tracks if track.track_type == track_type]

    @property
    def general_tracks(self) -> List[Track]:
        """
        :return: All :class:`Track`\\s of type ``General``.
        :rtype: list of :class:`Track`\\s
        """
        return self._tracks("General")

    @property
    def video_tracks(self) -> List[Track]:
        """
        :return: All :class:`Track`\\s of type ``Video``.
        :rtype: list of :class:`Track`\\s
        """
        return self._tracks("Video")

    @property
    def audio_tracks(self) -> List[Track]:
        """
        :return: All :class:`Track`\\s of type ``Audio``.
        :rtype: list of :class:`Track`\\s
        """
        return self._tracks("Audio")

    @property
    def text_tracks(self) -> List[Track]:
        """
        :return: All :class:`Track`\\s of type ``Text``.
        :rtype: list of :class:`Track`\\s
        """
        return self._tracks("Text")

    @property
    def other_tracks(self) -> List[Track]:
        """
        :return: All :class:`Track`\\s of type ``Other``.
        :rtype: list of :class:`Track`\\s
        """
        return self._tracks("Other")

    @property
    def image_tracks(self) -> List[Track]:
        """
        :return: All :class:`Track`\\s of type ``Image``.
        :rtype: list of :class:`Track`\\s
        """
        return self._tracks("Image")

    @property
    def menu_tracks(self) -> List[Track]:
        """
        :return: All :class:`Track`\\s of type ``Menu``.
        :rtype: list of :class:`Track`\\s
        """
        return self._tracks("Menu")

    def to_data(self) -> Dict[str, Any]:
        """
        Returns a dict representation of the object's :py:class:`Tracks <Track>`.
        :rtype: dict
        """
        return {"tracks": [_.to_data() for _ in self.tracks]}

    def to_json(self) -> str:
        """
        Returns a JSON representation of the object's :py:class:`Tracks <Track>`.
        :rtype: str
        """
        return json.dumps(self.to_data())


def get_media_info(bucket: str, key: str) -> MediaInfo:
    """
    Get media info of an S3 media object
    :param bucket:  S3 bucket name
    :param key:     S3 Key name
    :return:        Signed URL
    """
    signed_url = _get_signed_url(bucket, key)
    mi_output = json.loads(
        subprocess.check_output(["/opt/bin/mediainfo", "--full", "--output=JSON", signed_url]),
        cls=_Decoder
    )
    mi_output = _format_keys(mi_output)

    logger.info("Mediainfo(%s): %s" % (get_source(bucket, key), json.dumps(mi_output, indent=2)))
    return MediaInfo(mi_output)


def _get_signed_url(bucket: str, key: str) -> str:
    """
    Generate a signed URL
    :param bucket:  S3 bucket name
    :param key:     S3 Key name
    :return:        Signed URL
    """
    presigned_url = boto3.client("s3").generate_presigned_url('get_object',
                                                              Params={'Bucket': bucket, 'Key': key},
                                                              ExpiresIn=SIGNED_URL_EXPIRATION)
    return presigned_url


def _format_keys(x) -> any:
    if isinstance(x, list):
        return [_format_keys(v) for v in x]
    elif isinstance(x, dict):
        return {k.lower().strip().strip('_'): _format_keys(v) for k, v in x.items()}
    else:
        return x
