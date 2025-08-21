"""
Utility functions for status-related operations.

This module contains helper functions for processing and determining status information
for cameras, systems, and stores.
"""
from typing import Tuple, List, Dict, Any


def determine_status_message(result_camera: list, result_system: list) -> Tuple[str, str, str]:
    """
    Determine the status message and category color based on camera and system status.
    
    Args:
        result_camera: List of camera status records
        result_system: List of system status records
        
    Returns:
        Tuple of (camera_message, system_message, category_color)
    """
    camera_message = ""
    system_message = ""
    category_color = "GREEN"
    
    # Determine camera status message
    camera_message = _determine_camera_status(result_camera)
    
    # Determine system status message and update camera message if needed
    system_message, camera_message = _determine_system_status(result_system, camera_message)
    
    # Determine category color based on status messages
    category_color = _determine_category_color(camera_message, system_message)
    
    return camera_message, system_message, category_color


def _determine_camera_status(result_camera: list) -> str:
    """
    Determine the camera status message based on camera status records.
    
    Args:
        result_camera: List of camera status records
        
    Returns:
        Camera status message
    """
    if not result_camera:
        return "No cameras are working"
    elif all(result_camera):
        return "All cameras are working"
    elif any(result_camera):
        return "Partial cameras are working"
    else:
        return "No cameras are working"


def _determine_system_status(result_system: list, camera_message: str) -> Tuple[str, str]:
    """
    Determine the system status message and update camera message if needed.
    
    Args:
        result_system: List of system status records
        camera_message: Current camera status message
        
    Returns:
        Tuple of (system_message, updated_camera_message)
    """
    if not result_system:
        return "System not responding", "Unable to check cameras status"
    elif all(result_system):
        return "System responding", camera_message
    else:
        return "System not responding", "Unable to check cameras status"


def _determine_category_color(camera_message: str, system_message: str) -> str:
    """
    Determine the category color based on camera and system status messages.
    
    Args:
        camera_message: Camera status message
        system_message: System status message
        
    Returns:
        Category color
    """
    if system_message == "System not responding" or camera_message == "No cameras are working":
        return "RED"
    elif camera_message == "Partial cameras are working":
        return "YELLOW"
    elif camera_message == "All cameras are working" and system_message == "System responding":
        return "GREEN"
    else:
        return "RED"  # Default to RED for any unexpected status combination


def get_issue_message(camera_message: str, system_message: str) -> str:
    """
    Get the issue message based on camera and system status messages.
    
    Args:
        camera_message: Camera status message
        system_message: System status message
        
    Returns:
        Issue message
    """
    if system_message == "System not responding":
        return "System not responding"
    elif camera_message == "No cameras are working":
        return "No cameras are working"
    elif camera_message == "All cameras are working" and system_message == "System responding":
        return "Everything is working"
    elif camera_message == "Partial cameras are working":
        return "Partial cameras are working"
    else:
        return "Unknown issue"  # Default message for any unexpected status combination 